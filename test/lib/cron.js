'use strict';

const { assert } = require('chai');
const cron = require('../../lib/cron.js');
const hash = require('string-hash');

const evaluateHash = (jobId, min, max) => (hash(jobId) % ((max + 1) - min)) + min;

describe('cron', () => {
    const jobId = '123';

    // Evaluate the hashes for the default minutes and hours field
    const minutesHash = evaluateHash(jobId, 0, 59);
    const hoursHash = evaluateHash(jobId, 0, 23);

    it('should throw if the cron expession does not have 5 fields', () => {
        let cronExp;
        // 6 fields

        cronExp = '1 2 3 4 5 6';
        assert.throws(() => cron.transform(cronExp, jobId),
            Error, '1 2 3 4 5 6 does not have exactly 5 fields');

        // 4 fields
        cronExp = '1 2 3 4';
        assert.throws(() => cron.transform(cronExp, jobId),
            Error, '1 2 3 4 does not have exactly 5 fields');
    });

    it('should transform a cron expression with valid H symbol(s)', () => {
        let cronExp;

        // H * * * *
        cronExp = 'H * * * *';
        assert.deepEqual(cron.transform(cronExp, jobId), `${minutesHash} * * * *`);

        // * H/2 * * *
        cronExp = '* H/2 * * *';
        assert.deepEqual(cron.transform(cronExp, jobId),
            `${minutesHash} ${hoursHash}/2 * * *`);

        // * H(0-5) * * *
        cronExp = '* H(0-5) * * *';
        assert.deepEqual(cron.transform(cronExp, jobId),
            `${minutesHash} ${evaluateHash(jobId, 0, 5)} * * *`);
    });

    it('should throw if the cron expression has an invalid range value', () => {
        const cronExp = '* H(99-100) * * *';

        assert.throws(() => cron.transform(cronExp, jobId),
            Error, 'H(99-100) has an invalid range, expected range 0-23');
    });
});
