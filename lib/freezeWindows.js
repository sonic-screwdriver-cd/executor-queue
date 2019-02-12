'use strict';

const parser = require('cron-parser');

/**
 * Find the smallest missing integer in a range.
 * If there's a missing integer greater than element return that
 * Otherwise return the smallest missing integer lesser than element
 * @method findLatestMissing
 * @param {Array} array      Array of sorted numbers
 * @param {Integer} element  Element to compare against
 * @param {Integer} start    Start of range
 * @param {Integer} end      End of range
 * @return {Integer}         Return smallest missing integer
 */
const findLatestMissing = (array, element, start, end) => {
    let firstMissing;
    let firstMissingAfter;
    let offset = start;

    for (let i = start; i <= end; i += 1) {
        if (array[i - offset] !== i) {
            if (i < element) {
                if (firstMissing === undefined) {
                    firstMissing = i;
                }
            } else if (i > element) {
                if (firstMissingAfter === undefined) {
                    firstMissingAfter = i;
                    break;
                }
            }
            offset += 1;
        }
    }

    if (firstMissingAfter === undefined) {
        return firstMissing;
    }

    return firstMissingAfter;
};

/**
 * Return latest time outside of cron window for the given date.
 * @method timeOutOfWindow
 * @param  {String} cronExp Cron expression
 * @param  {Date}   date    JavaScript Date object to check if in window
 * @return {Boolean}        Date Object of latest time out of window
 */
const timeOutOfWindow = (cronExp, timeToCheck) => {
    const fields = cronExp.trim().split(/\s+/);

    // The seconds field is not allowed (e.g. '* * * * * *')
    if (fields.length !== 5) {
        throw new Error(`${cronExp} does not have exactly 5 fields`);
    }

    if (fields[2] !== '?' && fields[4] !== '?') {
        throw new Error(`${cronExp} cannot contain both days of month and week`);
    }

    const newCronExp = `${fields[0]} ${fields[1]} ${fields[2] === '?' ? '*' : fields[2]}
 ${fields[3]} ${fields[4] === '?' ? '*' : fields[4]}`;

    // Perform final validation before returning
    const cronObj = parser.parseExpression(newCronExp);
    let latest;

    const utcMinutes = timeToCheck.getUTCMinutes();
    const utcHours = timeToCheck.getUTCHours();
    const utcDayOfMonth = timeToCheck.getUTCDate();
    const utcDayOfWeek = timeToCheck.getUTCDay();
    const utcMonth = timeToCheck.getUTCMonth() + 1;

    /* eslint no-underscore-dangle: ["error", { "allow": ["_fields"] }] */
    const minuteField = cronObj._fields.minute;
    const hourField = cronObj._fields.hour;
    const dayOfMonthField = cronObj._fields.dayOfMonth;
    const dayOfWeekField = cronObj._fields.dayOfWeek;
    const monthField = cronObj._fields.month;

    const includesMinute = minuteField.includes(utcMinutes);
    const includesHour = hourField.includes(utcHours);
    const includesDayOfMonth = fields[2] === '?' || dayOfMonthField.includes(utcDayOfMonth);
    const includesDayOfWeek = fields[4] === '?' || dayOfWeekField.includes(utcDayOfWeek);
    const includesMonth = monthField.includes(utcMonth);

    const inWindow = [includesMinute,
        includesHour,
        includesDayOfMonth,
        includesDayOfWeek,
        includesMonth].every(Boolean);

    if (!inWindow) { return timeToCheck; }

    if (includesMinute && minuteField.length !== 60) {
        latest = findLatestMissing(minuteField, utcMinutes, 0, 59);
        timeToCheck.setUTCMinutes(latest);
        if (latest < utcMinutes) {
            timeToCheck.setUTCHours(timeToCheck.getUTCHours() + 1);
        }

        return timeToCheck;
    } else if (!includesMinute && minuteField.length !== 60) {
        return timeToCheck;
    }

    if (includesHour && hourField.length !== 24) {
        latest = findLatestMissing(hourField, utcHours, 0, 23);
        timeToCheck.setUTCHours(latest);
        if (latest < utcHours) {
            timeToCheck.setUTCDate(timeToCheck.getUTCDate() + 1);
        }
        timeToCheck.setUTCMinutes(0);

        return timeToCheck;
    } else if (!includesHour && hourField.length !== 24) {
        return timeToCheck;
    }

    if (includesDayOfMonth && fields[2] !== '?' && dayOfMonthField.length !== 31) {
        latest = findLatestMissing(dayOfMonthField, utcDayOfMonth, 1, 31);
        timeToCheck.setUTCDate(latest);
        if (latest < utcDayOfMonth) {
            timeToCheck.setUTCMonth(timeToCheck.getUTCMonth() + 1);
        }
        timeToCheck.setUTCMinutes(0);
        timeToCheck.setUTCHours(0);

        return timeToCheck;
    } else if (!includesDayOfMonth && fields[2] !== '?' && dayOfMonthField.length !== 31) {
        return timeToCheck;
    }

    if (includesDayOfWeek && fields[4] !== '?' && dayOfWeekField.length !== 8) {
        latest = findLatestMissing(dayOfWeekField, utcDayOfWeek, 0, 6);
        timeToCheck.setUTCDate((timeToCheck.getUTCDate() + latest) - utcDayOfWeek);
        if (latest < utcDayOfWeek) {
            timeToCheck.setUTCDate(timeToCheck.getUTCDate() + 7);
        }

        return timeToCheck;
    } else if (!includesDayOfWeek && fields[4] !== '?' && dayOfWeekField.length !== 8) {
        return timeToCheck;
    }

    if (includesMonth && monthField.length !== 12) {
        latest = findLatestMissing(monthField, utcMonth, 1, 12);
        timeToCheck.setUTCMonth(latest - 1);
        if (latest < utcMonth) {
            timeToCheck.setUTCFullYear(timeToCheck.getUTCFullYear() + 1);
        }
        timeToCheck.setUTCMinutes(0);
        timeToCheck.setUTCHours(0);
        timeToCheck.setUTCDate(1);
    }

    return timeToCheck;
};

/**
 * Return latest time outside of all freeze windows.
 * @method timeOutOfWindows
 * @param  {Array}  freezeWindows  Array of cron expressions of freeze windows
 * @param  {Date}   date           JavaScript Date object to check if in all windows
 * @return {Date}                  Date Object of latest time out of all windows
 */
const timeOutOfWindows = (freezeWindows, date) => {
    let idx = 0;

    while (idx < freezeWindows.length) {
        freezeWindows.map(freezeWindow =>
            timeOutOfWindow(freezeWindow, date)
        );
        idx += 1;
    }

    return date;
};

module.exports = {
    timeOutOfWindow,
    timeOutOfWindows
};
