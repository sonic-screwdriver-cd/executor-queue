'use strict';

const parser = require('cron-parser');
const stringHash = require('string-hash');

/**
 * Evaluate a numeric hash to a number within the range of min and max
 *
 * @method evaluateHash
 * @param  {Number} hash  Hash to evaluate
 * @param  {Number} min   Minimum evaluated value
 * @param  {String} max   Maximum evaluated value
 * @return {Number}       Evaluated hash
 */
const evaluateHash = (hash, min, max) => (hash % ((max + 1) - min)) + min;

/**
 * Transform a cron value containing a valid 'H' symbol into a valid cron value
 * @method transformValue
 * @param  {String} cronValue   Value to transform
 * @param  {Number} min         Minimum acceptable value
 * @param  {Number} max         Maximum acceptable value
 * @param  {Number} hashValue   Numeric hash to determine new value
 * @return {String}             Transformed cron value
 */
const transformValue = (cronValue, min, max, hashValue) => {
    const values = cronValue.split(',');

    // Transform each ',' seperated value
    // Ignore values that do not have a valid 'H' symbol
    values.forEach((value, i) => {
        // 'H' should evaluate to some value within the range (e.g. [0-59])
        if (value === 'H') {
            values[i] = evaluateHash(hashValue, min, max);

            return;
        }

        // e.g. H/5 -> #/5
        if (value.match(/H\/\d+/)) {
            values[i] = value.replace('H', evaluateHash(hashValue, min, max));

            return;
        }

        // e.g. H(0-5) -> #
        if (value.match(/H\(\d+-\d+\)/)) {
            const newMin = Number(value.substring(2, value.lastIndexOf('-')));
            const newMax = Number(value.substring(value.lastIndexOf('-') + 1,
                value.lastIndexOf(')')));

            // Range is invalid, throw an error
            if (newMin < min || newMax > max || newMin > newMax) {
                throw new Error(`${value} has an invalid range, expected range ${min}-${max}`);
            }

            values[i] = evaluateHash(hashValue, newMin, newMax);
        }
    });

    return values.join(',');
};

/**
 * Transform a cron expression containing valid 'H' symbol(s) into a valid cron expression
 * @method transformCron
 * @param  {String} cronExp Cron expression to transform
 * @param  {Number} jobId   Job ID
 * @return {String}         Transformed cron expression
 */
const transformCron = (cronExp, jobId) => {
    const fields = cronExp.trim().split(/\s+/);

    // The seconds field is not allowed (e.g. '* * * * * *')
    if (fields.length !== 5) {
        throw new Error(`${cronExp} does not have exactly 5 fields`);
    }

    const jobIdHash = stringHash(jobId.toString());

    // Minutes [0-59]
    // Always treat the minutes value as 'H'
    fields[0] = transformValue('H', 0, 59, jobIdHash);
    // Hours [0-23]
    fields[1] = transformValue(fields[1], 0, 23, jobIdHash);
    // Day of month [1-31]
    fields[2] = transformValue(fields[2], 1, 31, jobIdHash);
    // Months [1-12]
    fields[3] = transformValue(fields[3], 1, 12, jobIdHash);
    // Day of week [0-6]
    fields[4] = transformValue(fields[4], 0, 6, jobIdHash);

    const newCronExp = fields.join(' ');

    // Perform final validation before returning
    parser.parseExpression(newCronExp);

    return newCronExp;
};

/**
 * Get the next time of execution based on the cron expression
 * @method nextExecution
 * @param  {String} cronExp Cron expression to calculate next execution time
 * @return {Number}         Epoch timestamp (time of next execution).
 */
const nextExecution = (cronExp) => {
    const interval = parser.parseExpression(cronExp);

    return interval.next().getTime();
};

module.exports = {
    transform: transformCron,
    next: nextExecution
};
