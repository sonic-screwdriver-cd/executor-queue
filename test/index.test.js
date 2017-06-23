'use strict';

/* eslint-disable no-underscore-dangle */

const chai = require('chai');
const assert = chai.assert;
const mockery = require('mockery');
const sinon = require('sinon');
const testExecutor = require('./data/testExecutor');

sinon.assert.expose(chai.assert, { prefix: '' });

describe('index test', () => {
    let Executor;
    let executor;
    let fsMock;
    let k8sExecutorMock;
    let exampleExecutorMock;
    const ecosystem = {
        api: 'http://api.com',
        store: 'http://store.com'
    };
    const examplePluginOptions = {
        example: {
            host: 'somehost',
            token: 'sometoken',
            jobsNamespace: 'somenamespace'
        },
        launchVersion: 'someversion',
        prefix: 'someprefix'
    };
    const k8sPluginOptions = {
        kubernetes: {
            host: 'K8S_HOST',
            token: 'K8S_TOKEN',
            jobsNamespace: 'K8S_JOBS_NAMESPACE'
        },
        launchVersion: 'LAUNCH_VERSION',
        prefix: 'EXECUTOR_PREFIX'
    };

    before(() => {
        mockery.enable({
            useCleanCache: true,
            warnOnUnregistered: false
        });
    });

    beforeEach(() => {
        fsMock = {
            readFileSync: sinon.stub()
        };

        fsMock.readFileSync.withArgs('/var/run/secrets/kubernetes.io/serviceaccount/token')
            .returns('api_key');

        k8sExecutorMock = {
            _start: sinon.stub()
        };
        exampleExecutorMock = {
            _start: sinon.stub()
        };

        mockery.registerMock('fs', fsMock);
        mockery.registerMock('screwdriver-executor-k8s', testExecutor(k8sExecutorMock));
        mockery.registerMock('screwdriver-executor-example', testExecutor(exampleExecutorMock));

        // eslint-disable-next-line global-require
        Executor = require('../index');

        executor = new Executor({
            ecosystem,
            executor: [
                {
                    name: 'k8s',
                    options: k8sPluginOptions
                },
                {
                    name: 'example',
                    options: examplePluginOptions
                }
            ]
        });
    });

    afterEach(() => {
        mockery.deregisterAll();
    });

    describe('construction', () => {
        let exampleOptions;
        let k8sOptions;

        beforeEach(() => {
            exampleOptions = {
                ecosystem,
                example: {
                    host: 'somehost',
                    token: 'sometoken',
                    jobsNamespace: 'somenamespace'
                },
                launchVersion: 'someversion',
                prefix: 'someprefix'
            };
            k8sOptions = {
                ecosystem,
                kubernetes: {
                    host: 'K8S_HOST',
                    token: 'K8S_TOKEN',
                    jobsNamespace: 'K8S_JOBS_NAMESPACE'
                },
                launchVersion: 'LAUNCH_VERSION',
                prefix: 'EXECUTOR_PREFIX'
            };
        });

        it('defaults to an empty object when the ecosystem does not exist', () => {
            executor = new Executor({
                executor: [{
                    name: 'k8s',
                    options: k8sPluginOptions
                }]
            });

            const executorKubernetes = executor.k8s;

            k8sOptions.ecosystem = {};

            assert.deepEqual(executorKubernetes.constructorParams, k8sOptions);
        });

        it('defaults to an empty object when config does not exist', () => {
            const error = new Error('No executor config passed in.');

            try {
                executor = new Executor();
            } catch (err) {
                assert.deepEqual(err, error);

                return;
            }
            assert.fail();
        });

        it('throws an error when the executor config does not exist', () => {
            const error = new Error('No executor config passed in.');

            try {
                executor = new Executor({ ecosystem });
            } catch (err) {
                assert.deepEqual(err, error);

                return;
            }
            assert.fail();
        });

        it('throws an error when the executor config is not an array', () => {
            const error = new Error('No executor config passed in.');

            try {
                executor = new Executor({
                    ecosystem,
                    executor: {
                        name: 'k8s',
                        options: k8sPluginOptions
                    }
                });
            } catch (err) {
                assert.deepEqual(err, error);

                return;
            }
            assert.fail();
        });

        it('throws an error when the executor config is an empty array', () => {
            const error = new Error('No executor config passed in.');

            try {
                executor = new Executor({
                    ecosystem,
                    executor: []
                });
            } catch (err) {
                assert.deepEqual(err, error);

                return;
            }
            assert.fail();
        });

        it('throws an error when no default executor is set', () => {
            const error = new Error('No default executor set.');

            try {
                executor = new Executor({
                    ecosystem,
                    executor: [{
                        name: 'DNE'
                    },
                    {
                        name: 'DNE2',
                        options: k8sPluginOptions
                    }]
                });
            } catch (err) {
                assert.strictEqual(err.message, error.message);

                return;
            }
            assert.fail();
        });

        it('does not throw an error when a npm module cannot be registered', () => {
            try {
                executor = new Executor({
                    ecosystem,
                    executor: [{
                        name: 'DNE'
                    },
                    {
                        name: 'k8s',
                        options: k8sPluginOptions
                    }]
                });
            } catch (err) {
                assert.fail(err, '');
            }
        });

        it('registers multiple plugins', () => {
            const executorKubernetes = executor.k8s;
            const exampleExecutor = executor.example;

            assert.deepEqual(executorKubernetes.constructorParams, k8sOptions);
            assert.deepEqual(exampleExecutor.constructorParams, exampleOptions);
        });

        it('registers a single plugin', () => {
            executor = new Executor({
                ecosystem: {
                    api: 'http://api.com',
                    store: 'http://store.com'
                },
                executor: [{
                    name: 'k8s',
                    options: k8sPluginOptions
                }]
            });

            const executorKubernetes = executor.k8s;

            assert.deepEqual(executorKubernetes.constructorParams, k8sOptions);
        });
    });

    describe('start', () => {
        it('default executor when no annotation is given', () => {
            executor = new Executor({
                defaultPlugin: 'example',
                ecosystem,
                executor: [
                    {
                        name: 'k8s',
                        options: k8sPluginOptions
                    },
                    {
                        name: 'example',
                        options: examplePluginOptions
                    }
                ]
            });
            exampleExecutorMock._start.resolves('exampleExecutorMockResult');

            return executor.start({
                buildId: 920,
                container: 'node:4',
                apiUri: 'http://api.com',
                token: 'asdf'
            }).then((result) => {
                assert.strictEqual(result, 'exampleExecutorMockResult');
            });
        });

        it('default executor is the first one when given no executor annotation', () => {
            k8sExecutorMock._start.resolves('k8sExecutorResult');

            return executor.start({
                buildId: 920,
                container: 'node:4',
                apiUri: 'http://api.com',
                token: 'qwer'
            }).then((result) => {
                assert.strictEqual(result, 'k8sExecutorResult');
                assert.calledOnce(k8sExecutorMock._start);
                assert.notCalled(exampleExecutorMock._start);
            });
        });

        it('default executor is the first one when given an invalid executor annotation', () => {
            k8sExecutorMock._start.resolves('k8sExecutorResult');
            exampleExecutorMock._start.rejects();

            return executor.start({
                annotations: {
                    'beta.screwdriver.cd/executor': 'darrenIsSometimesRight'
                },
                buildId: 920,
                container: 'node:4',
                apiUri: 'http://api.com',
                token: 'qwer'
            }).then((result) => {
                assert.strictEqual(result, 'k8sExecutorResult');
                assert.calledOnce(k8sExecutorMock._start);
                assert.notCalled(exampleExecutorMock._start);
            });
        });

        it('uses an annotation to determine which executor to call', () => {
            k8sExecutorMock._start.rejects();
            exampleExecutorMock._start.resolves('exampleExecutorResult');

            return executor.start({
                annotations: {
                    'beta.screwdriver.cd/executor': 'example'
                },
                buildId: 920,
                container: 'node:4',
                apiUri: 'http://api.com',
                token: 'qwer'
            }).then((result) => {
                assert.strictEqual(result, 'exampleExecutorResult');
                assert.calledOnce(exampleExecutorMock._start);
                assert.notCalled(k8sExecutorMock._start);
            });
        });

        it('propogates the failure from initiating a start', () => {
            const testError = new Error('triggeredError');

            k8sExecutorMock._start.rejects(testError);

            return executor.start({
                annotations: {
                    'beta.screwdriver.cd/executor': 'screwdriver-executor-k8s'
                },
                buildId: 920,
                container: 'node:4',
                apiUri: 'http://api.com',
                token: 'qwer'
            }).then(assert.fail, (err) => {
                assert.deepEqual(err, testError);
            });
        });
    });
});
