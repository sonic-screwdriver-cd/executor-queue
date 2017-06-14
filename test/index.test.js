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
    const pluginOptions = {
        ecosystem: {
            api: 'http://api.com',
            store: 'http://store.com'
        }
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
        mockery.registerMock('my-example-executor', testExecutor(exampleExecutorMock));

        // eslint-disable-next-line global-require
        Executor = require('../index');

        executor = new Executor({
            plugins: [
                {
                    name: 'screwdriver-executor-k8s',
                    options: pluginOptions
                },
                {
                    name: 'my-example-executor',
                    options: pluginOptions
                }
            ]
        });
    });

    afterEach(() => {
        mockery.deregisterAll();
    });

    describe('construction', () => {
        it('registers multiple plugins', () => {
            const executorKubernetes = executor['screwdriver-executor-k8s'];
            const exampleExecutor = executor['my-example-executor'];

            assert.deepEqual(executorKubernetes.constructorParams, pluginOptions);
            assert.deepEqual(exampleExecutor.constructorParams, pluginOptions);
        });

        it('registers a single plugin', () => {
            executor = new Executor({
                plugins: {
                    name: 'screwdriver-executor-k8s',
                    options: pluginOptions
                }
            });

            const executorKubernetes = executor['screwdriver-executor-k8s'];

            assert.deepEqual(executorKubernetes.constructorParams, pluginOptions);
        });
    });

    describe('start', () => {
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
                    'beta.screwdriver.cd/executor': 'my-example-executor'
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
