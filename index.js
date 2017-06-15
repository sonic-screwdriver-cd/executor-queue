'use strict';

// Key in annotations object that maps to an executor NPM module
// value is a placeholder until resolution to screwdriver-cd/screwdriver#595
const ANNOTATION_EXECUTOR_TYPE = 'beta.screwdriver.cd/executor';
const Executor = require('screwdriver-executor-base');

class ExecutorRouter extends Executor {
    /**
     * Constructs a router for different Executor strategies.
     * @method constructor
     * @param  {Object}         options
     * @param  {Array|String}   options.plugins                Array of plugins to load or a single plugin string
     * @param  {String}         options.plugins[x].moduleName  Name of the executor NPM module to load
     * @param  {String}         options.plugins[x].options     Configuration to construct the module with
     */
    constructor(options = {}) {
        super();

        const plugins = options.plugins;
        const registrations = (Array.isArray(plugins)) ? plugins : [plugins];

        registrations.forEach((plugin, index) => {
            // eslint-disable-next-line global-require, import/no-dynamic-require
            const ExecutorPlugin = require(plugin.name);

            const executorPlugin = new ExecutorPlugin(plugin.options);

            // Set the default executor
            if (index === 0) {
                this.default_executor = executorPlugin;
            }

            this[plugin.name] = executorPlugin;
        });
    }

    /**
     * Starts a new build in an executor
     * @method start
     * @param {Object} config               Configuration
     * @param {Object} [config.annotations] Optional key/value object
     * @param {String} config.apiUri        Screwdriver's API
     * @param {String} config.buildId       Unique ID for a build
     * @param {String} config.container     Container for the build to run in
     * @param {String} config.token         JWT to act on behalf of the build
     * @return {Promise}
     */
    start(config) {
        const annotations = config.annotations || {};
        const executorType = annotations[ANNOTATION_EXECUTOR_TYPE];
        // Route to executor specified in annotations or use default executor
        const executor = this[executorType] || this.default_executor;

        return executor.start(config);
    }
}

module.exports = ExecutorRouter;
