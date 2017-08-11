'use strict';

const Executor = require('screwdriver-executor-base');
const Resque = require('node-resque');
const fuses = require('circuit-fuses');
const Breaker = fuses.breaker;
const Fusebox = fuses.box;

class ExecutorQueue extends Executor {
    /**
     * Constructs a router for different Executor strategies.
     * @method constructor
     * @param  {Object}         config                      Object with executor and ecosystem
     * @param  {Object}         config.redisConnection      Connection details for redis
     * @param  {Object}         [config.breaker]            optional breaker config
     */
    constructor(config = {}) {
        if (!config.redisConnection) {
            throw new Error('No redis connection passed in');
        }

        const breaker = Object.assign({}, config.breaker || {});

        super();

        const redisConnection = Object.assign({}, config.redisConnection, { pkg: 'ioredis' });

        // eslint-disable-next-line new-cap
        this.queue = new Resque.queue({ connection: redisConnection });
        this.connectBreaker = new Breaker((...args) => this.queue.connect(...args), breaker);
        this.enqueueBreaker = new Breaker((...args) => this.queue.enqueue(...args), breaker);

        this.fusebox = new Fusebox();
        this.fusebox.addFuse(this.connectBreaker);
        this.fusebox.addFuse(this.enqueueBreaker);
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
        return this.connectBreaker.runCommand()
            // Note: arguments to enqueue are [queue name, job type, array of args]
            .then(() => this.enqueueBreaker.runCommand('builds', 'start', [config]));
    }

    /**
     * Retrieve stats for the executor
     * @method stats
     * @param {Response} Object     Object containing stats for the executor
     */
    stats() {
        return this.enqueueBreaker.stats();
    }
}

module.exports = ExecutorQueue;
