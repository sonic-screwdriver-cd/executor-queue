'use strict';

const Executor = require('screwdriver-executor-base');
const Redis = require('ioredis');
const Resque = require('node-resque');
const fuses = require('circuit-fuses');
const requestretry = require('requestretry');
const hoek = require('hoek');
const winston = require('winston');
const cron = require('./lib/cron');
const timeOutOfWindows = require('./lib/freezeWindows').timeOutOfWindows;
const Breaker = fuses.breaker;
const FuseBox = fuses.box;
const EXPIRE_TIME = 1800; // 30 mins
const RETRY_LIMIT = 3;
const RETRY_DELAY = 5;

class ExecutorQueue extends Executor {
    /**
     * Constructs a router for different Executor strategies.
     * @method constructor
     * @param  {Object}         config                      Object with executor and ecosystem
     * @param  {Object}         config.redisConnection      Connection details for redis
     * @param  {Object}         config.pipelineFactory      Pipeline Factory instance
     * @param  {String}         [config.prefix]             Prefix for queue name
     * @param  {Object}         [config.breaker]            Optional breaker config
     */
    constructor(config = {}) {
        if (!config.redisConnection) {
            throw new Error('No redis connection passed in');
        }
        if (!config.pipelineFactory) {
            throw new Error('No PipelineFactory instance passed in');
        }

        const breaker = Object.assign({}, config.breaker || {});

        super();

        this.prefix = config.prefix || '';
        this.buildQueue = `${this.prefix}builds`;
        this.periodicBuildQueue = `${this.prefix}periodicBuilds`;
        this.frozenBuildQueue = `${this.prefix}frozenBuilds`;
        this.buildConfigTable = `${this.prefix}buildConfigs`;
        this.periodicBuildTable = `${this.prefix}periodicBuildConfigs`;
        this.frozenBuildTable = `${this.prefix}frozenBuildConfigs`;
        this.tokenGen = null;
        this.userTokenGen = null;
        this.pipelineFactory = config.pipelineFactory;

        const redisConnection = Object.assign({}, config.redisConnection, { pkg: 'ioredis' });

        this.redis = new Redis(
            redisConnection.port,
            redisConnection.host,
            redisConnection.options
        );

        // eslint-disable-next-line new-cap
        // queue機能を使うためにqueueに代入
        this.queue = new Resque.Queue({ connection: redisConnection });
        this.queueBreaker = new Breaker((funcName, ...args) => {
            const callback = args.pop();

            this.queue[funcName](...args)
                .then((...results) => callback(null, ...results))
                .catch(callback);
        }, breaker);
        this.redisBreaker = new Breaker((funcName, ...args) =>
            // Use the queue's built-in connection to send redis commands instead of instantiating a new one
            this.redis[funcName](...args), breaker);
        this.requestRetryStrategy = (err, response) =>
            !!err || (response.statusCode !== 201 && response.statusCode !== 200);

        this.fuseBox = new FuseBox();
        this.fuseBox.addFuse(this.queueBreaker);
        this.fuseBox.addFuse(this.redisBreaker);

        const retryOptions = {
            plugins: ['Retry'],
            pluginOptions: {
                Retry: {
                    retryLimit: RETRY_LIMIT,
                    retryDelay: RETRY_DELAY
                }
            }
        };
        // Jobs object to register the worker with
        const jobs = {
            startDelayed: Object.assign({ perform: async (jobConfig) => {
                try {
                    const fullConfig = await this.redisBreaker
                        .runCommand('hget', this.periodicBuildTable, jobConfig.jobId);

                    return await this.startPeriodic(
                        Object.assign(JSON.parse(fullConfig), { triggerBuild: true }));
                } catch (err) {
                    winston.error('err in startDelayed job: ', err);
                    throw err;
                }
            } }, retryOptions),
            startFrozen: Object.assign({ perform: async (jobConfig) => {
                try {
                    const fullConfig = await this.redisBreaker
                        .runCommand('hget', this.frozenBuildTable, jobConfig.jobId);

                    return await this.startFrozen(JSON.parse(fullConfig));
                } catch (err) {
                    winston.error('err in startFrozen job: ', err);
                    throw err;
                }
            } }, retryOptions)
        };

        // eslint-disable-next-line new-cap
        this.multiWorker = new Resque.MultiWorker({
            connection: redisConnection,
            queues: [this.periodicBuildQueue, this.frozenBuildQueue],
            minTaskProcessors: 1,
            maxTaskProcessors: 10,
            checkTimeout: 1000,
            maxEventLoopDelay: 10,
            toDisconnectProcessors: true
        }, jobs);
        // eslint-disable-next-line new-cap
        this.scheduler = new Resque.Scheduler({ connection: redisConnection });

        this.multiWorker.on('start', workerId =>
            winston.info(`worker[${workerId}] started`));
        this.multiWorker.on('end', workerId =>
            winston.info(`worker[${workerId}] ended`));
        this.multiWorker.on('cleaning_worker', (workerId, worker, pid) =>
            winston.info(`cleaning old worker ${worker} pid ${pid}`));
        this.multiWorker.on('job', (workerId, queue, job) =>
            winston.info(`worker[${workerId}] working job ${queue} ${JSON.stringify(job)}`));
        this.multiWorker.on('reEnqueue', (workerId, queue, job, plugin) =>
            // eslint-disable-next-line max-len
            winston.info(`worker[${workerId}] reEnqueue job (${plugin}) ${queue} ${JSON.stringify(job)}`));
        this.multiWorker.on('success', (workerId, queue, job, result) =>
            // eslint-disable-next-line max-len
            winston.info(`worker[${workerId}] job success ${queue} ${JSON.stringify(job)} >> ${result}`));
        this.multiWorker.on('failure', (workerId, queue, job, failure) =>
            // eslint-disable-next-line max-len
            winston.info(`worker[${workerId}] job failure ${queue} ${JSON.stringify(job)} >> ${failure}`));
        this.multiWorker.on('error', (workerId, queue, job, error) =>
            winston.error(`worker[${workerId}] error ${queue} ${JSON.stringify(job)} >> ${error}`));

        // multiWorker emitters
        this.multiWorker.on('internalError', error =>
            winston.error(error));

        this.scheduler.on('start', () =>
            winston.info('scheduler started'));
        this.scheduler.on('end', () =>
            winston.info('scheduler ended'));
        this.scheduler.on('master', state =>
            winston.info(`scheduler became master ${state}`));
        this.scheduler.on('error', error =>
            winston.info(`scheduler error >> ${error}`));
        this.scheduler.on('workingTimestamp', timestamp =>
            winston.info(`scheduler working timestamp ${timestamp}`));
        this.scheduler.on('transferredJob', (timestamp, job) =>
            winston.info(`scheduler enqueuing job timestamp  >>  ${JSON.stringify(job)}`));

        this.multiWorker.start();
        this.scheduler.connect(() => {
            this.scheduler.start();
        });

        process.on('SIGTERM', () => {
            this.multiWorker.end((error) => {
                if (error) {
                    winston.error(`failed to end the worker: ${error}`);
                }

                this.scheduler.end((err) => {
                    if (err) {
                        winston.error(`failed to end the scheduler: ${err}`);
                        process.exit(128);
                    }
                    process.exit(0);
                });
            });
        });
    }

    /**
     * Posts a new build event to the API
     * @method postBuildEvent
     * @param {Object} config           Configuration
     * @param {Number} [config.eventId] Optional Parent event ID (optional)
     * @param {Object} config.pipeline  Pipeline of the job
     * @param {Object} config.job       Job object to create periodic builds for
     * @param {String} config.apiUri    Base URL of the Screwdriver API
     * @return {Promise}
     */
    async postBuildEvent({ pipeline, job, apiUri, eventId, causeMessage }) {
        const pipelineInstance = await this.pipelineFactory.get(pipeline.id);
        const admin = await pipelineInstance.getFirstAdmin();
        const jwt = this.userTokenGen(admin.username, {}, pipeline.scmContext);

        winston.info(`POST event for pipeline ${pipeline.id}:${job.name}` +
            `using user ${admin.username}`);
        const options = {
            url: `${apiUri}/v4/events`,
            method: 'POST',
            headers: {
                Authorization: `Bearer ${jwt}`,
                'Content-Type': 'application/json'
            },
            json: true,
            body: {
                pipelineId: pipeline.id,
                startFrom: job.name,
                creator: {
                    name: 'Screwdriver scheduler',
                    username: 'sd:scheduler'
                },
                causeMessage: causeMessage || 'Automatically started by scheduler'
            },
            maxAttempts: RETRY_LIMIT,
            retryDelay: RETRY_DELAY * 1000, // in ms
            retryStrategy: this.requestRetryStrategy
        };

        if (eventId) {
            options.body.parentEventId = eventId;
        }

        return new Promise((resolve, reject) => {
            requestretry(options, (err, response) => {
                if (!err && response.statusCode === 201) {
                    return resolve(response);
                }

                if (response.statusCode !== 201) {
                    return reject(JSON.stringify(response.body));
                }

                return reject(err);
            });
        });
    }

    async updateBuildStatus({ buildId, status, statusMessage, token, apiUri }) {
        const options = {
            json: true,
            method: 'PUT',
            uri: `${apiUri}/v4/builds/${buildId}`,
            body: {
                status,
                statusMessage
            },
            headers: {
                Authorization: `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            maxAttempts: RETRY_LIMIT,
            retryDelay: RETRY_DELAY * 1000, // in ms
            retryStrategy: this.requestRetryStrategy
        };

        return new Promise((resolve, reject) => {
            requestretry(options, (err, response) => {
                if (!err && response.statusCode === 200) {
                    return resolve(response);
                }

                if (response.statusCode !== 200) {
                    return reject(JSON.stringify(response.body));
                }

                return reject(err);
            });
        });
    }

    /**
     * Starts a new periodic build in an executor
     * @method _startPeriodic
     * @param {Object}   config              Configuration
     * @param {Object}   config.pipeline     Pipeline of the job
     * @param {Object}   config.job          Job object to create periodic builds for
     * @param {String}   config.apiUri       Base URL of the Screwdriver API
     * @param {Function} config.tokenGen     Function to generate JWT from username, scope and scmContext
     * @param {Boolean}  config.isUpdate     Boolean to determine if updating existing periodic build
     * @param {Boolean}  config.triggerBuild Flag to post new build event
     * @return {Promise}
     */
    async _startPeriodic(config) {
        const { job, tokenGen, isUpdate, triggerBuild } = config;
        // eslint-disable-next-line max-len
        const buildCron = hoek.reach(job, 'permutations>0>annotations>screwdriver.cd/buildPeriodically',
            { separator: '>' });

        // Save tokenGen to current executor object so we can access it in postBuildEvent
        if (!this.userTokenGen) {
            this.userTokenGen = tokenGen;
        }

        if (isUpdate) {
            // eslint-disable-next-line no-underscore-dangle
            await this._stopPeriodic({ jobId: job.id });
        }

        if (triggerBuild) {
            config.causeMessage = 'Started by periodic build scheduler';

            // Even if post event failed for this event after retry, we should still enqueue the next event
            try {
                await this.postBuildEvent(config);
            } catch (err) {
                winston.error(`failed to post build event for job ${job.id}: ${err}`);
            }
        }

        if (buildCron && job.state === 'ENABLED' && !job.archived) {
            await this.connect();

            const next = cron.next(cron.transform(buildCron, job.id));

            // Store the config in redis
            await this.redisBreaker.runCommand('hset', this.periodicBuildTable,
                job.id, JSON.stringify(Object.assign(config, {
                    isUpdate: false,
                    triggerBuild: false
                })));

            // Note: arguments to enqueueAt are [timestamp, queue name, job name, array of args]
            let shouldRetry = false;

            try {
                try {
                    await this.queue.enqueueAt(next, this.periodicBuildQueue,
                        'startDelayed', [{ jobId: job.id }]);
                } catch (err) {
                    // Error thrown by node-resque if there is duplicate: https://github.com/taskrabbit/node-resque/blob/master/lib/queue.js#L65
                    // eslint-disable-next-line max-len
                    if (err && err.message !== 'Job already enqueued at this time with same arguments') {
                        shouldRetry = true;
                    }
                }
                if (!shouldRetry) {
                    return Promise.resolve();
                }

                await this.queueBreaker.runCommand('enqueueAt', next,
                    this.periodicBuildQueue, 'startDelayed', [{ jobId: job.id }]);
            } catch (err) {
                winston.error(`failed to add to delayed queue for job ${job.id}: ${err}`);
            }
        }

        return Promise.resolve();
    }

    /**
     * Stops a previously scheduled periodic build in an executor
     * @async  _stopPeriodic
     * @param  {Object}  config        Configuration
     * @param  {Integer} config.jobId  ID of the job with periodic builds
     * @return {Promise}
     */
    async _stopPeriodic(config) {
        await this.connect();

        await this.queueBreaker.runCommand('delDelayed', this.periodicBuildQueue, 'startDelayed', [{
            jobId: config.jobId
        }]);

        return this.redisBreaker.runCommand('hdel', this.periodicBuildTable, config.jobId);
    }

    /**
     * Calls postBuildEvent() with job configuration
     * @async _startFrozen
     * @param {Object} config       Configuration
     * @return {Promise}
     */
    async _startFrozen(config) {
        const newConfig = {
            job: {
                name: config.jobName
            },
            causeMessage: 'Started by freeze window scheduler'
        };

        Object.assign(newConfig, config);

        return this.postBuildEvent(newConfig)
            .catch((err) => {
                winston.err(`failed to post build event for job ${config.jobId}: ${err}`);

                return Promise.resolve();
            });
    }

    /**
     * Stops a previously enqueued frozen build in an executor
     * @async  stopFrozen
     * @param  {Object}  config        Configuration
     * @param  {Integer} config.jobId  ID of the job with frozen builds
     * @return {Promise}
     */
    async _stopFrozen(config) {
        await this.connect();

        await this.queueBreaker.runCommand('delDelayed', this.frozenBuildQueue, 'startFrozen', [{
            jobId: config.jobId
        }]);

        return this.redisBreaker.runCommand('hdel', this.frozenBuildTable, config.jobId);
    }

    /**
     * Starts a new build in an executor
     * @async  _start
     * @param  {Object} config               Configuration
     * @param  {Object} [config.annotations] Optional key/value object
     * @param  {Number} [config.eventId]     Optional eventID that this build belongs to
     * @param  {String} config.build         Build object
     * @param  {Array}  config.blockedBy     Array of job IDs that this job is blocked by. Always blockedby itself
     * @param  {Array}  config.freezeWindows Array of cron expressions that this job cannot run during
     * @param  {String} config.apiUri        Screwdriver's API
     * @param  {String} config.jobId         JobID that this build belongs to
     * @param  {String} config.jobName       Name of job that this build belongs to
     * @param  {String} config.buildId       Unique ID for a build
     * @param  {Object} config.pipeline      Pipeline of the job
     * @param  {Fn}     config.tokenGen      Function to generate JWT from username, scope and scmContext
     * @param  {String} config.container     Container for the build to run in
     * @param  {String} config.token         JWT to act on behalf of the build
     * @return {Promise}
     */
    async _start(config) {
        await this.connect();
        const { build, buildId, jobId, blockedBy, freezeWindows, token, apiUri } = config;

        if (!this.tokenGen) {
            this.tokenGen = config.tokenGen;
        }

        delete config.build;

        // eslint-disable-next-line no-underscore-dangle
        await this._stopFrozen({
            jobId
        });

        const currentTime = new Date();
        const origTime = new Date(currentTime.getTime());

        timeOutOfWindows(freezeWindows, currentTime);

        let enq;

        if (currentTime.getTime() > origTime.getTime()) {
            await this.updateBuildStatus({
                buildId,
                token,
                apiUri,
                status: 'FROZEN',
                statusMessage: `Blocked by freeze window, re-enqueued to ${currentTime}`
            }).catch((err) => {
                winston.error(`failed to update build status for build ${buildId}: ${err}`);

                return Promise.resolve();
            });

            await this.queueBreaker.runCommand('delDelayed', this.frozenBuildQueue,
                'startFrozen', [{
                    jobId
                }]);

            await this.redisBreaker.runCommand('hset', this.frozenBuildTable,
                jobId, JSON.stringify(config));

            enq = await this.queueBreaker.runCommand('enqueueAt', currentTime.getTime(),
                this.frozenBuildQueue, 'startFrozen', [{
                    jobId
                }]
            );
        } else {
            // Store the config in redis
            await this.redisBreaker.runCommand('hset', this.buildConfigTable,
                buildId, JSON.stringify(config));

            // Note: arguments to enqueue are [queue name, job name, array of args]
            enq = await this.queueBreaker.runCommand('enqueue', this.buildQueue, 'start', [{
                buildId,
                jobId,
                blockedBy: blockedBy.toString()
            }]);
        }

        // for backward compatibility
        if (build && build.stats) {
            // need to reassign so the field can be dirty
            build.stats = hoek.merge(build.stats, { queueEnterTime: (new Date()).toISOString() });
            await build.update();
        }

        return enq;
    }

    /**
     * Stop a running or finished build
     * @async  _stop
     * @param  {Object} config               Configuration
     * @param  {Array}  config.blockedBy     Array of job IDs that this job is blocked by. Always blockedby itself
     * @param  {String} config.buildId       Unique ID for a build
     * @param  {String} config.jobId         JobID that this build belongs to
     * @return {Promise}
     */
    async _stop(config) {
        await this.connect();

        const { buildId, jobId } = config; // in case config contains something else

        let blockedBy;

        if (config.blockedBy !== undefined) {
            blockedBy = config.blockedBy.toString();
        }

        const numDeleted = await this.queueBreaker.runCommand('del', this.buildQueue, 'start', [{
            buildId,
            jobId,
            blockedBy
        }]);
        const deleteKey = `deleted_${jobId}_${buildId}`;
        let started = true;

        // This is to prevent the case where a build is aborted while still in buildQueue
        // The job might be picked up by the worker, so it's not deleted from buildQueue here
        // Immediately after, the job gets put back to the queue, so it's always inside buildQueue
        // This key will be cleaned up automatically or when it's picked up by the worker
        await this.redisBreaker.runCommand('set', deleteKey, '');
        await this.redisBreaker.runCommand('expire', deleteKey, EXPIRE_TIME);

        if (numDeleted !== 0) { // build hasn't started
            started = false;
        }

        return this.queueBreaker.runCommand('enqueue', this.buildQueue, 'stop', [{
            buildId,
            jobId,
            blockedBy,
            started // call executor.stop if the job already started
        }]);
    }

    /**
     * Connect to the queue if we haven't already
     * @method connect
     * @return {Promise}
     */
    connect() {
        if (this.queue.connection.connected) {
            return Promise.resolve();
        }

        return this.queueBreaker.runCommand('connect');
    }

    /**
     * Retrieve stats for the executor
     * @method stats
     * @param {Response} Object     Object containing stats for the executor
     */
    stats() {
        return this.queueBreaker.stats();
    }
}

module.exports = ExecutorQueue;
