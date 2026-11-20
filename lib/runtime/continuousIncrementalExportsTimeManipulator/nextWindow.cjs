'use strict';

// Builds the time Lambda's response. CommonJS so jest (ts-jest, CommonJS) can test it;
// index.mjs imports it, and the folder is deployed as-is with no bundling.

const MINUTE_MS = 60 * 1000;

// DynamoDB accepts incremental export windows from 15 minutes to 24 hours.
const MAX_EXPORT_WINDOW_MS = 24 * 60 * MINUTE_MS;

// Tuning: a member aims for the first group time at least this long after its window start.
// It keeps every window clear of DynamoDB's 15-minute minimum while stored times carry milliseconds.
const MIN_WINDOW_TO_GROUP_TIME_MS = 30 * MINUTE_MS;

const SyncGroupState = Object.freeze({
    ON_GROUP_TIME: 'ON_GROUP_TIME',
    REALIGNING: 'REALIGNING',
    CATCHING_UP: 'CATCHING_UP',
});

/** Minutes after midnight UTC for a two-digit `HH:MM` group time. */
function parseGroupTimeUtc(value) {
    const match = /^([01]\d|2[0-3]):([0-5]\d)$/.exec(String(value));
    if (!match) {
        throw new Error(`syncGroupTimeUtc '${value}' is not a two-digit HH:MM time between 00:00 and 23:59`);
    }
    return Number(match[1]) * 60 + Number(match[2]);
}

/**
 * Window end for a sync group member whose last window ended at startMs.
 * Group times repeat every window from the configured time of day.
 */
function memberWindowEnd(startMs, windowMs, groupTimeUtc, nowMs) {
    const anchorMs = parseGroupTimeUtc(groupTimeUtc) * MINUTE_MS;
    const onGroupTime = (startMs - anchorMs) % windowMs === 0;

    // T: the first group time at least 30 minutes after the start.
    const groupTimeMs = anchorMs + Math.ceil((startMs + MIN_WINDOW_TO_GROUP_TIME_MS - anchorMs) / windowMs) * windowMs;

    // Catching up: T and the group time after it have both passed. A full window
    // brings the data closest to now; for a member on the group time it ends at T.
    if (nowMs >= groupTimeMs + windowMs) {
        return {
            endMs: startMs + windowMs,
            state: onGroupTime ? SyncGroupState.ON_GROUP_TIME : SyncGroupState.CATCHING_UP,
        };
    }

    const state = onGroupTime ? SyncGroupState.ON_GROUP_TIME : SyncGroupState.REALIGNING;

    // A start within 30 minutes before a group time cannot reach T in one window: split it.
    if (groupTimeMs - startMs > MAX_EXPORT_WINDOW_MS) {
        return { endMs: groupTimeMs - windowMs / 2, state };
    }

    return { endMs: groupTimeMs, state };
}

/**
 * The time Lambda's response body for an event and clock, plus the values its log lines report.
 * Without `syncGroupTimeUtc` the window ends a full window after the last export.
 */
function computeNextWindow(event, now) {
    const incrementalExportWindowSizeInMinutes = event.incrementalExportWindowSizeInMinutes;
    const windowMs = incrementalExportWindowSizeInMinutes * MINUTE_MS;
    const lastExportTime = new Date(event.lastExportTime);
    const syncGroupTimeUtc = event.syncGroupTimeUtc;

    let resultDate;
    let syncGroupState;
    if (syncGroupTimeUtc === undefined || syncGroupTimeUtc === null) {
        resultDate = new Date(lastExportTime.getTime() + windowMs);
    } else {
        const member = memberWindowEnd(lastExportTime.getTime(), windowMs, syncGroupTimeUtc, now.getTime());
        resultDate = new Date(member.endMs);
        syncGroupState = member.state;
    }

    const timeDiffFromLastExportToNow = now - lastExportTime;
    const timeDiffFromLastExportToNowInMinutes = timeDiffFromLastExportToNow / (1000 * 60);
    const timeDiffFromLastExportToNowInMinutesInBlocks = timeDiffFromLastExportToNowInMinutes / incrementalExportWindowSizeInMinutes;
    const incrementalBlocksBehind = Math.floor(timeDiffFromLastExportToNowInMinutesInBlocks) - 1; // the current window is always discarded

    let prefixCmd = '';
    if (event.bucketPrefix !== '') {
        prefixCmd = ` --s3-prefix ${event.bucketPrefix}`;
    }

    const remedy = `aws dynamodb export-table-to-point-in-time --table-arn ${event.tableArn} --s3-bucket-owner ${event.bucketOwner} --s3-bucket ${event.bucket}${prefixCmd} --export-format ${event.exportFormat} --export-type INCREMENTAL_EXPORT --incremental-export-specification ExportFromTime=${Math.floor(lastExportTime / 1000)},ExportToTime=${Math.floor(resultDate / 1000)},ExportViewType=${event.exportViewType}`;

    const body = {
        lastExportTime: lastExportTime,
        incrementalExportWindowSizeInMinutes: incrementalExportWindowSizeInMinutes,
        durationAddedStartTime: resultDate,
        incrementalBlocksBehind: incrementalBlocksBehind,
        remedy: remedy,
    };
    if (syncGroupState !== undefined) {
        body.syncGroupState = syncGroupState;
    }

    return {
        body,
        details: {
            timeDiffFromLastExportToNow,
            timeDiffFromLastExportToNowInMinutes,
            timeDiffFromLastExportToNowInMinutesInBlocks,
            syncGroupTimeUtc,
        },
    };
}

module.exports = { computeNextWindow, SyncGroupState };
