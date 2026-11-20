import nextWindow from './nextWindow.cjs';

const { computeNextWindow } = nextWindow;

export const handler = async (event, context) => {

    console.log(event);

    const executionId = event.executionId;
    const currentDateTime = new Date();
    const { body, details } = computeNextWindow(event, currentDateTime);

    console.log(`Id:${executionId} - Current time:${currentDateTime}`);
    console.log(`Id:${executionId} - Time Diff:${details.timeDiffFromLastExportToNow}`);
    console.log(`Id:${executionId} - Time Diff in Mins:${details.timeDiffFromLastExportToNowInMinutes}`);
    console.log(`Id:${executionId} - Time Diff in Mins in blocks:${details.timeDiffFromLastExportToNowInMinutesInBlocks}`);
    console.log(`Id:${executionId} - Incremental blocks behind:${body.incrementalBlocksBehind}`);

    if (body.syncGroupState !== undefined) {
        console.log(`Id:${executionId} - Sync group time ${details.syncGroupTimeUtc} UTC, window ${body.lastExportTime.toISOString()} to ${body.durationAddedStartTime.toISOString()}, ${body.syncGroupState}`);
    }

    return {
        statusCode: 200,
        body: body
    };
};
