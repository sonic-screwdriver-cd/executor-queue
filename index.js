'use strict';

const Executor = require('screwdriver-executor-base');
const Resque = require('node-resque');
const Breaker = require('circuit-fuses').breaker;

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

        const redisConnection = Object.assign({}, config.redisConnection, { pkg: 'ioredis' });

        // eslint-disable-next-line new-cap
        this.queue = new Resque.queue({ connection: redisConnection });
        this.breaker = new Breaker((funcName, ...args) => this.queue[funcName](...args), breaker);
    }

    /**
     * Starts a new build in an executor
     * @method _start
     * @param {Object} config               Configuration
     * @param {Object} [config.annotations] Optional key/value object
     * @param {String} config.apiUri        Screwdriver's API
     * @param {String} config.buildId       Unique ID for a build
     * @param {String} config.container     Container for the build to run in
     * @param {String} config.token         JWT to act on behalf of the build
     * @return {Promise}
     */
    _start(config) {
        return this.connect()
            // Note: arguments to enqueue are [queue name, job type, array of args]
            .then(() => this.breaker.runCommand('enqueue', this.buildQueue, 'start', [config]));
    }

    /**
     * Stop a running or finished build
     * @method _stop
     * @param {Object} config               Configuration
     * @param {Object} [config.annotations] Optional key/value object
     * @param {String} config.buildId       Unique ID for a build
     * @return {Promise}
     */
    _stop(config) {
        return this.connect()
            .then(() => this.breaker.runCommand('del', this.buildQueue, 'start', [config]))
            .then((numDeleted) => {
                if (numDeleted !== 0) {
                    // Build hadn't been started, "start" event was removed from queue
                    return null;
                }

                // "start" event has been processed, need worker to stop the executor
                return this.breaker.runCommand('enqueue', this.buildQueue, 'stop', [config]);
            });
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

        return this.breaker.runCommand('connect');
    }

    /**
     * Retrieve stats for the executor
     * @method stats
     * @param {Response} Object     Object containing stats for the executor
     */
    stats() {
        return this.breaker.stats();
    }
}

module.exports = ExecutorQueue;
