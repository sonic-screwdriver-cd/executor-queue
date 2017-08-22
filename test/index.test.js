'use strict';

/* eslint-disable no-underscore-dangle */

const chai = require('chai');
const assert = chai.assert;
const mockery = require('mockery');
const sinon = require('sinon');
const testConnection = require('./data/testConnection.json');
const testConfig = require('./data/fullConfig.json');
const partialTestConfig = {
    buildId: testConfig.buildId
};

sinon.assert.expose(chai.assert, { prefix: '' });

describe('index test', () => {
    let Executor;
    let executor;
    let resqueMock;
    let queueMock;
    let redisMock;
    let redisConstructorMock;

    before(() => {
        mockery.enable({
            useCleanCache: true,
            warnOnUnregistered: false
        });
    });

    beforeEach(() => {
        queueMock = {
            connect: sinon.stub().yieldsAsync(),
            enqueue: sinon.stub().yieldsAsync(),
            del: sinon.stub().yieldsAsync(null, 1),
            connection: {
                connected: false
            }
        };
        resqueMock = {
            queue: sinon.stub().returns(queueMock)
        };
        redisMock = {
            hdel: sinon.stub().yieldsAsync(),
            hset: sinon.stub().yieldsAsync()
        };
        redisConstructorMock = sinon.stub().returns(redisMock);

        mockery.registerMock('node-resque', resqueMock);
        mockery.registerMock('ioredis', redisConstructorMock);

        /* eslint-disable global-require */
        Executor = require('../index');
        /* eslint-enable global-require */

        executor = new Executor({
            redisConnection: testConnection,
            breaker: {
                retry: {
                    retries: 1
                }
            }
        });
    });

    afterEach(() => {
        mockery.deregisterAll();
        mockery.resetCache();
    });

    after(() => {
        mockery.disable();
    });

    describe('construction', () => {
        it('constructs the executor', () => {
            assert.instanceOf(executor, Executor);
        });

        it('constructs the executor when no breaker config is passed in', () => {
            executor = new Executor({
                redisConnection: testConnection
            });

            assert.instanceOf(executor, Executor);
        });

        it('takes in a prefix', () => {
            executor = new Executor({
                redisConnection: testConnection,
                prefix: 'beta_'
            });

            assert.instanceOf(executor, Executor);
            assert.strictEqual(executor.prefix, 'beta_');
            assert.strictEqual(executor.buildQueue, 'beta_builds');
            assert.strictEqual(executor.buildConfigTable, 'beta_buildConfigs');
        });

        it('throws when not given a redis connection', () => {
            assert.throws(() => new Executor(), 'No redis connection passed in');
        });
    });

    describe('_start', () => {
        it('rejects if it can\'t establish a connection', function () {
            queueMock.connect.yieldsAsync(new Error('couldn\'t connect'));

            return executor.start({
                annotations: {
                    'beta.screwdriver.cd/executor': 'screwdriver-executor-k8s'
                },
                buildId: 8609,
                container: 'node:4',
                apiUri: 'http://api.com',
                token: 'asdf'
            }).then(() => {
                assert.fail('Should not get here');
            }, (err) => {
                assert.instanceOf(err, Error);
            });
        });

        it('enqueues a build and caches the config', () => executor.start({
            annotations: {
                'beta.screwdriver.cd/executor': 'screwdriver-executor-k8s'
            },
            buildId: 8609,
            container: 'node:4',
            apiUri: 'http://api.com',
            token: 'asdf'
        }).then(() => {
            assert.calledOnce(queueMock.connect);
            assert.calledWith(redisMock.hset, 'buildConfigs', testConfig.buildId,
                JSON.stringify(testConfig));
            assert.calledWith(queueMock.enqueue, 'builds', 'start', [partialTestConfig]);
        }));

        it('doesn\'t call connect if there\'s already a connection', () => {
            queueMock.connection.connected = true;

            return executor.start({
                annotations: {
                    'beta.screwdriver.cd/executor': 'screwdriver-executor-k8s'
                },
                buildId: 8609,
                container: 'node:4',
                apiUri: 'http://api.com',
                token: 'asdf'
            }).then(() => {
                assert.notCalled(queueMock.connect);
                assert.calledWith(queueMock.enqueue, 'builds', 'start', [partialTestConfig]);
            });
        });
    });

    describe('_stop', () => {
        it('rejects if it can\'t establish a connection', function () {
            queueMock.connect.yieldsAsync(new Error('couldn\'t connect'));

            return executor.stop({
                buildId: 8609
            }).then(() => {
                assert.fail('Should not get here');
            }, (err) => {
                assert.instanceOf(err, Error);
            });
        });

        it('removes a start event from the queue and the cached buildconfig', () => executor.stop({
            buildId: 8609
        }).then(() => {
            assert.calledOnce(queueMock.connect);
            assert.calledWith(queueMock.del, 'builds', 'start', [partialTestConfig]);
            assert.calledWith(redisMock.hdel, 'buildConfigs', 8609);
            assert.notCalled(queueMock.enqueue);
        }));

        it('adds a stop event to the queue if no start events were removed', () => {
            queueMock.del.yieldsAsync(null, 0);

            return executor.stop({
                buildId: 8609
            }).then(() => {
                assert.calledOnce(queueMock.connect);
                assert.calledWith(queueMock.del, 'builds', 'start', [partialTestConfig]);
                assert.calledWith(queueMock.enqueue, 'builds', 'stop', [partialTestConfig]);
            });
        });

        it('doesn\'t call connect if there\'s already a connection', () => {
            queueMock.connection.connected = true;

            return executor.stop({
                annotations: {
                    'beta.screwdriver.cd/executor': 'screwdriver-executor-k8s'
                },
                buildId: 8609
            }).then(() => {
                assert.notCalled(queueMock.connect);
                assert.calledWith(queueMock.del, 'builds', 'start', [partialTestConfig]);
            });
        });
    });

    describe('stats', () => {
        it('returns the correct stats', () => {
            assert.deepEqual(executor.stats(), {
                requests: {
                    total: 0,
                    timeouts: 0,
                    success: 0,
                    failure: 0,
                    concurrent: 0,
                    averageTime: 0
                },
                breaker: {
                    isClosed: true
                }
            });
        });
    });
});
