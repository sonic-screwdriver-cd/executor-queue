'use strict';

/* eslint-disable no-underscore-dangle */

const chai = require('chai');
const util = require('util');
const assert = chai.assert;
const mockery = require('mockery');
const sinon = require('sinon');
const testConnection = require('./data/testConnection.json');
const testConfig = require('./data/fullConfig.json');
const testPipeline = require('./data/testPipeline.json');
const testJob = require('./data/testJob.json');
const partialTestConfig = {
    buildId: testConfig.buildId
};
const tokenGen = sinon.stub.returns('123456abc');
const testDelayedConfig = {
    pipeline: testPipeline,
    job: testJob,
    tokenGen
};
const EventEmitter = require('events').EventEmitter;

sinon.assert.expose(chai.assert, { prefix: '' });

describe('index test', () => {
    let Executor;
    let executor;
    let multiWorker;
    let spyMultiWorker;
    let scheduler;
    let spyScheduler;
    let resqueMock;
    let queueMock;
    let redisMock;
    let redisConstructorMock;
    let cronMock;
    let winstonMock;

    before(() => {
        mockery.enable({
            useCleanCache: true,
            warnOnUnregistered: false
        });
    });

    beforeEach(() => {
        multiWorker = function () {
            this.start = () => {};
            this.end = sinon.stub();
        };
        scheduler = function () {
            this.start = () => {};
            this.connect = () => {};
            this.end = sinon.stub();
        };
        util.inherits(multiWorker, EventEmitter);
        util.inherits(scheduler, EventEmitter);
        queueMock = {
            connect: sinon.stub().yieldsAsync(),
            enqueue: sinon.stub().yieldsAsync(),
            enqueueAt: sinon.stub().yieldsAsync(),
            del: sinon.stub().yieldsAsync(null, 1),
            delDelayed: sinon.stub().yieldsAsync(null, 1),
            connection: {
                connected: false
            }
        };
        resqueMock = {
            queue: sinon.stub().returns(queueMock),
            multiWorker,
            scheduler
        };
        spyMultiWorker = sinon.spy(resqueMock, 'multiWorker');
        spyScheduler = sinon.spy(resqueMock, 'scheduler');
        winstonMock = {
            info: sinon.stub(),
            error: sinon.stub()
        };
        redisMock = {
            hdel: sinon.stub().yieldsAsync(),
            hset: sinon.stub().yieldsAsync()
        };
        redisConstructorMock = sinon.stub().returns(redisMock);
        cronMock = {
            transform: sinon.stub().returns('H H H H H'),
            next: sinon.stub().returns(1500000)
        };

        mockery.registerMock('node-resque', resqueMock);
        mockery.registerMock('ioredis', redisConstructorMock);
        mockery.registerMock('./lib/cron', cronMock);
        mockery.registerMock('winston', winstonMock);

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

        it('constructs the multiWorker', () => {
            const expectedConfig = {
                connection: testConnection,
                queues: ['periodicBuilds'],
                minTaskProcessors: 1,
                maxTaskProcessors: 10,
                checkTimeout: 1000,
                maxEventLoopDelay: 10,
                toDisconnectProcessors: true
            };

            assert.calledWith(spyMultiWorker, sinon.match(expectedConfig), sinon.match.any);
        });

        it('constructs the scheduler', () => {
            const expectedConfig = {
                connection: testConnection
            };

            assert.calledWith(spyScheduler, sinon.match(expectedConfig));
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

    describe('_startPeriodic', () => {
        it('rejects if it can\'t establish a connection', function () {
            queueMock.connect.yieldsAsync(new Error('couldn\'t connect'));

            return executor.startPeriodic(testDelayedConfig).then(() => {
                assert.fail('Should not get here');
            }, (err) => {
                assert.instanceOf(err, Error);
            });
        });

        it('doesn\'t call connect if there\'s already a connection', () => {
            queueMock.connection.connected = true;

            return executor.startPeriodic(testDelayedConfig).then(() => {
                assert.notCalled(queueMock.connect);
            });
        });

        it('enqueues a new delayed job in the queue', () => {
            executor.startPeriodic(testDelayedConfig).then(() => {
                assert.calledOnce(queueMock.connect);
                assert.calledWith(redisMock.hset, 'periodicBuildConfigs', testJob.id,
                    JSON.stringify(testDelayedConfig));
                assert.calledWith(cronMock.transform, '* * * * *', testJob.id);
                assert.calledWith(cronMock.next, 'H H H H H');
                assert.calledWith(queueMock.enqueueAt, 1500000, 'periodicBuilds', 'startDelayed', [{
                    jobId: testJob.id
                }]);
            });
        });

        it('stops and reEnqueues an existing job if isUpdate flag is passed', () => {
            testDelayedConfig.isUpdate = true;
            executor.startPeriodic(testDelayedConfig).then(() => {
                assert.calledTwice(queueMock.connect);
                assert.calledWith(redisMock.hset, 'periodicBuildConfigs', testJob.id,
                    JSON.stringify(testDelayedConfig));
                assert.calledWith(queueMock.enqueueAt, 1500000, 'periodicBuilds', 'startDelayed', [{
                    jobId: testJob.id
                }]);
                assert.calledWith(queueMock.delDelayed, 'periodicBuilds', 'startDelayed', [{
                    jobId: testJob.id
                }]);
                assert.calledWith(redisMock.hdel, 'periodicBuildConfigs', testJob.id);
            });
        });

        it('stops but does not reEnqueue an existing job if it is disabled', () => {
            testDelayedConfig.isUpdate = true;
            testDelayedConfig.job.state = 'DISABLED';
            testDelayedConfig.job.archived = false;
            executor.startPeriodic(testDelayedConfig).then(() => {
                assert.calledOnce(queueMock.connect);
                assert.notCalled(redisMock.hset);
                assert.notCalled(queueMock.enqueueAt);
                assert.calledWith(queueMock.delDelayed, 'periodicBuilds', 'startDelayed', [{
                    jobId: testJob.id
                }]);
                assert.calledWith(redisMock.hdel, 'periodicBuildConfigs', testJob.id);
            });
        });

        it('stops but does not reEnqueue an existing job if it is archived', () => {
            testDelayedConfig.isUpdate = true;
            testDelayedConfig.job.state = 'ENABLED';
            testDelayedConfig.job.archived = true;
            executor.startPeriodic(testDelayedConfig).then(() => {
                assert.calledOnce(queueMock.connect);
                assert.notCalled(redisMock.hset);
                assert.notCalled(queueMock.enqueueAt);
                assert.calledWith(queueMock.delDelayed, 'periodicBuilds', 'startDelayed', [{
                    jobId: testJob.id
                }]);
                assert.calledWith(redisMock.hdel, 'periodicBuildConfigs', testJob.id);
            });
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
