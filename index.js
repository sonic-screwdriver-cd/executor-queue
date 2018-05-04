'use strict';

const Executor = require('screwdriver-executor-base');
const Redis = require('ioredis');
const Resque = require('node-resque');
const fuses = require('circuit-fuses');
const req = require('request');
const hoek = require('hoek');
const winston = require('winston');
const cron = require('./lib/cron');
const Breaker = fuses.breaker;
const FuseBox = fuses.box;

class ExecutorQueue extends Executor {
    /**
     * Constructs a router for different Executor strategies.
     * @method constructor
     * @param  {Object}         config                      Object with executor and ecosystem
     * @param  {Object}         config.redisConnection      Connection details for redis
     * @param  {String}         [config.prefix]             Prefix for queue name
     * @param  {Object}         [config.breaker]            Optional breaker config
     */
    constructor(config = {}) {
        if (!config.redisConnection) {
            throw new Error('No redis connection passed in');
        }

        const breaker = Object.assign({}, config.breaker || {});

        super();

        this.prefix = config.prefix || '';
        this.buildQueue = `${this.prefix}builds`;
        this.periodicBuildQueue = `${this.prefix}periodicBuilds`;
        this.buildConfigTable = `${this.prefix}buildConfigs`;
        this.periodicBuildTable = `${this.prefix}periodicBuildConfigs`;
        this.tokenGen = null;

        const redisConnection = Object.assign({}, config.redisConnection, { pkg: 'ioredis' });

        this.redis = new Redis(
            redisConnection.port,
            redisConnection.host,
            redisConnection.options
        );

        // eslint-disable-next-line new-cap
        this.queue = new Resque.queue({ connection: redisConnection });
        this.queueBreaker = new Breaker((funcName, ...args) =>
            this.queue[funcName](...args), breaker);
        this.redisBreaker = new Breaker((funcName, ...args) =>
            // Use the queue's built-in connection to send redis commands instead of instantiating a new one
            this.redis[funcName](...args), breaker);

        this.fuseBox = new FuseBox();
        this.fuseBox.addFuse(this.queueBreaker);
        this.fuseBox.addFuse(this.redisBreaker);

        const RETRY_LIMIT = 3;
        const RETRY_DELAY = 5;
        const retryOptions = {
            plugins: ['retry'],
            pluginOptions: {
                retry: {
                    retryLimit: RETRY_LIMIT,
                    retryDelay: RETRY_DELAY
                }
            }
        };
        // Jobs object to register the worker with
        const jobs = {
            startDelayed: Object.assign({ perform: (jobConfig, callback) =>
                this.redisBreaker.runCommand('hget', this.periodicBuildTable,
                    jobConfig.jobId)
                    .then(fullConfig => this.startPeriodic(Object.assign(JSON.parse(fullConfig),
                        { triggerBuild: true })))
                    .then(result => callback(null, result), (err) => {
                        winston.error('err in startDelayed job: ', err);
                        callback(err);
                    })
            }, retryOptions)
        };

        // eslint-disable-next-line new-cap
        this.multiWorker = new Resque.multiWorker({
            connection: redisConnection,
            queues: [this.periodicBuildQueue],
            minTaskProcessors: 1,
            maxTaskProcessors: 10,
            checkTimeout: 1000,
            maxEventLoopDelay: 10,
            toDisconnectProcessors: true
        }, jobs);
        // eslint-disable-next-line new-cap
        this.scheduler = new Resque.scheduler({ connection: redisConnection });

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
        this.scheduler.on('working_timestamp', timestamp =>
            winston.info(`scheduler working timestamp ${timestamp}`));
        this.scheduler.on('transferred_job', (timestamp, job) =>
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
     * @param {Object} config          Configuration
     * @param {Object} config.pipeline Pipeline of the job
     * @param {Object} config.job      Job object to create periodic builds for
     * @return {Promise}
     */
    async postBuildEvent({ pipeline, job }) {
        const jwt = this.tokenGen(Object.keys(pipeline.admins)[0], {}, pipeline.scmContext);

        console.log('QUEUE postBuildEvent: jwt: ', jwt);

        const options = {
            url: 'https://beta.api.screwdriver.cd/v4/events',
            method: 'POST',
            headers: {
                Authorization: `Bearer ${jwt}`,
                'Content-Type': 'application/json'
            },
            json: true,
            body: {
                pipelineId: pipeline.id,
                startFrom: job.name
            }
        };

        return req(options, (err, response) => {
            console.log('QUEUE req: err:', JSON.stringify(err));
            console.log('QUEUE req: response:', JSON.stringify(response));
            if (!err && response.statusCode === 201) {
                return Promise.resolve(response);
            }

            return Promise.reject(err);
        });
    }

    /**
     * Starts a new periodic build in an executor
     * @method _startPeriodic
     * @param {Object}   config              Configuration
     * @param {Object}   config.pipeline     Pipeline of the job
     * @param {Object}   config.job          Job object to create periodic builds for
     * @param {Function} config.tokenGen     Function to generate JWT from username, scope and scmContext
     * @param {Boolean}  config.isUpdate     Boolean to determine if updating existing periodic build
     * @param {Boolean}  config.triggerBuild Flag to post new build event
     * @return {Promise}
     */
    async _startPeriodic(config) {
        // eslint-disable-next-line max-len
        const buildCron = hoek.reach(config.job, 'permutations>0>annotations>screwdriver.cd/buildPeriodically',
            { separator: '>' });

        // Save tokenGen to current executor object so we can access it in postBuildEvent
        if (!this.tokenGen) {
            this.tokenGen = config.tokenGen;
        }

        if (config.isUpdate) {
            console.log('QUEUE isUpdate: Stopping periodic job');

            // eslint-disable-next-line no-underscore-dangle
            await this._stopPeriodic({
                jobId: config.job.id
            });
        }

        if (config.triggerBuild) {
            console.log('QUEUE triggerBuild: Posting Build Event');
            await this.postBuildEvent(config)
                .catch(() => Promise.resolve());
        }

        if (buildCron) {
            await this.connect();

            const next = cron.next(cron.transform(buildCron, config.job.id));

            console.log('QUEUE buildCron: Running hset command on redis table');
            // Store the config in redis
            await this.redisBreaker.runCommand('hset', this.periodicBuildTable,
                config.job.id, JSON.stringify(Object.assign(config, {
                    isUpdate: false,
                    triggerBuild: false
                })));

            console.log('QUEUE buildCron: Running enqueueAt on redis queue');

            // Note: arguments to enqueueAt are [timestamp, queue name, job name, array of args]
            return this.queueBreaker.runCommand('enqueueAt', next,
                this.periodicBuildQueue, 'startDelayed', [{
                    jobId: config.job.id
                }])
                .catch(() => Promise.resolve());
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

        console.log('QUEUE: stopPeriodic: running delDelayed on queue: ', config.jobId);
        await this.queueBreaker.runCommand('delDelayed', this.periodicBuildQueue, 'startDelayed', [{
            jobId: config.jobId
        }]);

        console.log('QUEUE: stopPeriodic: running hdel on table', config.jobId);

        return this.redisBreaker.runCommand('hdel', this.periodicBuildTable, config.jobId);
    }

    /**
     * Starts a new build in an executor
     * @async  _start
     * @param  {Object} config               Configuration
     * @param  {Object} [config.annotations] Optional key/value object
     * @param  {String} config.apiUri        Screwdriver's API
     * @param  {String} config.buildId       Unique ID for a build
     * @param  {String} config.container     Container for the build to run in
     * @param  {String} config.token         JWT to act on behalf of the build
     * @return {Promise}
     */
    async _start(config) {
        await this.connect();

        // Store the config in redis
        await this.redisBreaker.runCommand('hset', this.buildConfigTable,
            config.buildId, JSON.stringify(config));

        // Note: arguments to enqueue are [queue name, job name, array of args]
        return this.queueBreaker.runCommand('enqueue', this.buildQueue, 'start', [{
            buildId: config.buildId
        }]);
    }

    /**
     * Stop a running or finished build
     * @async  _stop
     * @param  {Object} config               Configuration
     * @param  {String} config.buildId       Unique ID for a build
     * @return {Promise}
     */
    async _stop(config) {
        await this.connect();

        const numDeleted = await this.queueBreaker.runCommand('del', this.buildQueue, 'start', [{
            buildId: config.buildId
        }]);

        if (numDeleted !== 0) {
            // Build hadn't been started, "start" event was removed from queue
            return this.redisBreaker.runCommand('hdel', this.buildConfigTable, config.buildId);
        }

        // "start" event has been processed, need worker to stop the executor
        return this.queueBreaker.runCommand('enqueue', this.buildQueue, 'stop', [{
            buildId: config.buildId
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
