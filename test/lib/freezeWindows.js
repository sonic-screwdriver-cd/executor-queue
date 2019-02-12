'use strict';

const { assert } = require('chai');
const timeOutOfWindows = require('../../lib/freezeWindows.js').timeOutOfWindows;

describe('freeze windows', () => {
    it('should return the correct date outside the freeze windows', () => {
        const currentDate = new Date(Date.UTC(2018, 10, 24, 10, 33));

        timeOutOfWindows([
            '0-31,33-35 * * * ?',
            '* 5-23 * * ?',
            '* * ? 11 *',
            '* * ? * 6'
        ], currentDate);

        const expectedDate = new Date('2018-12-02T00:32:00.000Z');

        assert.equal(currentDate.getTime(), expectedDate.getTime());
    });

    it('should return the same date if outside the freeze windows', () => {
        const currentDate = new Date(Date.UTC(2018, 10, 24, 10, 33));

        timeOutOfWindows([
            '0-31,34-35 * * * ?',
            '* 11-17 * * ?',
            '* * ? 10 *',
            '* * ? * 4'
        ], currentDate);

        const expectedDate = new Date('2018-11-24T10:33:00.000Z');

        assert.equal(currentDate.getTime(), expectedDate.getTime());
    });
});
