'use strict';

const ExecutorBase = require('screwdriver-executor-base');

module.exports = (stubsMap) => {
    /**
     * Generic executor class for testing
     * @type {Class}
     */
    const TestExecutorClass = class TestExecutor extends ExecutorBase {
        constructor(options) {
            super();

            this.options = options;

            Object.keys(stubsMap).forEach((key) => {
                this[key] = stubsMap[key];
            });
        }

        get constructorParams() {
            return this.options;
        }
    };

    return TestExecutorClass;
};
