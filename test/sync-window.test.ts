import { execFileSync } from 'child_process';
import * as path from 'path';
import { pathToFileURL } from 'url';
import { isValidGroupTime } from '../lib/syncGroups';

interface ResponseBody {
  lastExportTime: Date;
  incrementalExportWindowSizeInMinutes: number;
  durationAddedStartTime: Date;
  incrementalBlocksBehind: number;
  remedy: string;
  syncGroupState?: string;
}

interface NextWindow {
  body: ResponseBody;
  details: {
    timeDiffFromLastExportToNow: number;
    timeDiffFromLastExportToNowInMinutes: number;
    timeDiffFromLastExportToNowInMinutesInBlocks: number;
    syncGroupTimeUtc?: string;
  };
}

const lambdaDir = path.join(__dirname, '..', 'lib', 'runtime', 'continuousIncrementalExportsTimeManipulator');
// The Lambda folder is deployed as-is, so the rule lives in a CommonJS file jest can require.
// eslint-disable-next-line @typescript-eslint/no-var-requires
const { computeNextWindow, SyncGroupState } = require(path.join(lambdaDir, 'nextWindow.cjs')) as {
  computeNextWindow: (event: Record<string, unknown>, now: Date) => NextWindow;
  SyncGroupState: Record<'ON_GROUP_TIME' | 'REALIGNING' | 'CATCHING_UP', string>;
};

const MINUTE = 60 * 1000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

const baseEvent = {
  executionId: 'arn:aws:states:us-west-2:123456789012:execution:sm:exec-1',
  incrementalExportWindowSizeInMinutes: 1440,
  tableArn: 'arn:aws:dynamodb:us-west-2:123456789012:table/ehawk-mobilehub-1778525198-Appointments',
  bucketOwner: '123456789012',
  bucket: 'export-bucket',
  bucketPrefix: 'ddb/appointments',
  exportFormat: 'DYNAMODB_JSON',
  exportViewType: 'NEW_AND_OLD_IMAGES',
};

const ms = (iso: string) => new Date(iso).getTime();

/** Runs the rule for a member and returns the window end as an ISO string. */
function memberRun(lastExportTime: string | number, now: string | number, groupTime = '21:00') {
  const result = computeNextWindow(
    { ...baseEvent, lastExportTime: new Date(lastExportTime).toISOString(), syncGroupTimeUtc: groupTime },
    new Date(now),
  );
  return { end: result.body.durationAddedStartTime.toISOString(), body: result.body };
}

const endOf = (lastExportTime: string | number, now: string | number, groupTime = '21:00') =>
  memberRun(lastExportTime, now, groupTime).end;

/** Reference copy of the handler's arithmetic from before sync groups, with the clock injected. */
function legacyBody(event: Record<string, any>, currentDateTime: Date) {
  const incrementalExportWindowSizeInMinutes = event.incrementalExportWindowSizeInMinutes;
  const lastExportTime = new Date(event.lastExportTime);
  const resultDate = new Date(lastExportTime.getTime() + (incrementalExportWindowSizeInMinutes * 60000));
  const timeDiffFromLastExportToNow = currentDateTime.getTime() - lastExportTime.getTime();
  const timeDiffFromLastExportToNowInMinutes = timeDiffFromLastExportToNow / (1000 * 60);
  const timeDiffFromLastExportToNowInMinutesInBlocks = timeDiffFromLastExportToNowInMinutes / incrementalExportWindowSizeInMinutes;
  const incrementalBlocksBehind = Math.floor(timeDiffFromLastExportToNowInMinutesInBlocks) - 1;
  let prefixCmd = '';
  if (event.bucketPrefix !== '') {
    prefixCmd = ` --s3-prefix ${event.bucketPrefix}`;
  }
  const remedy = `aws dynamodb export-table-to-point-in-time --table-arn ${event.tableArn} --s3-bucket-owner ${event.bucketOwner} --s3-bucket ${event.bucket}${prefixCmd} --export-format ${event.exportFormat} --export-type INCREMENTAL_EXPORT --incremental-export-specification ExportFromTime=${Math.floor(lastExportTime.getTime() / 1000)},ExportToTime=${Math.floor(resultDate.getTime() / 1000)},ExportViewType=${event.exportViewType}`;
  return {
    lastExportTime,
    incrementalExportWindowSizeInMinutes,
    durationAddedStartTime: resultDate,
    incrementalBlocksBehind,
    remedy,
  };
}

// Group time 21:00 UTC, a 1440-minute window, December 2026 (Mon 7th .. Fri 11th).
describe('sync group window rule', () => {
  it('keeps a member on the group time', () => {
    const run = memberRun('2026-12-08T21:00:00.000Z', '2026-12-09T21:05:00.000Z');
    expect(run.end).toBe('2026-12-09T21:00:00.000Z');
    expect(run.body.syncGroupState).toBe(SyncGroupState.ON_GROUP_TIME);
  });

  it('returns the coming group time before it is due, so the existing check skips the run', () => {
    expect(endOf('2026-12-08T21:00:00.000Z', '2026-12-09T13:05:00.000Z')).toBe('2026-12-09T21:00:00.000Z');
  });

  it('retries the same group window at a later check when the export call failed', () => {
    // No export started, so the stored position is still Tuesday 21:00.
    const run = memberRun('2026-12-08T21:00:00.000Z', '2026-12-10T05:05:00.000Z');
    expect(run.end).toBe('2026-12-09T21:00:00.000Z');
    expect(run.body.syncGroupState).toBe(SyncGroupState.ON_GROUP_TIME);
    expect(endOf(run.end, '2026-12-10T21:05:00.000Z')).toBe('2026-12-10T21:00:00.000Z');
  });

  it('realigns after a morning full export', () => {
    const start = '2026-12-09T09:00:00.412Z';
    expect(endOf(start, '2026-12-09T13:05:00.000Z')).toBe('2026-12-09T21:00:00.000Z');
    const run = memberRun(start, '2026-12-09T21:05:00.000Z');
    expect(run.end).toBe('2026-12-09T21:00:00.000Z');
    expect(run.body.syncGroupState).toBe(SyncGroupState.REALIGNING);
  });

  it('splits a start just before the group time into two windows', () => {
    expect(endOf('2026-12-09T20:50:00.000Z', '2026-12-09T21:05:00.000Z')).toBe('2026-12-10T09:00:00.000Z');
    const run = memberRun('2026-12-09T20:50:00.000Z', '2026-12-10T13:05:00.000Z');
    expect(run.end).toBe('2026-12-10T09:00:00.000Z');
    expect(run.body.syncGroupState).toBe(SyncGroupState.REALIGNING);
    expect(endOf('2026-12-10T09:00:00.000Z', '2026-12-10T21:05:00.000Z')).toBe('2026-12-10T21:00:00.000Z');
  });

  it('takes a 23 h 59 min window for a start one minute after the group time', () => {
    expect(endOf('2026-12-09T21:01:00.000Z', '2026-12-10T21:05:00.000Z')).toBe('2026-12-10T21:00:00.000Z');
  });

  it('realigns starts between a group time and its run', () => {
    expect(endOf('2026-12-09T21:02:00.000Z', '2026-12-10T21:05:00.000Z')).toBe('2026-12-10T21:00:00.000Z');
    expect(endOf('2026-12-09T21:05:01.000Z', '2026-12-10T21:05:01.500Z')).toBe('2026-12-10T21:00:00.000Z');
  });

  it('follows a group time moved earlier to 20:57', () => {
    expect(endOf('2026-12-09T21:00:00.000Z', '2026-12-10T21:02:00.000Z', '20:57')).toBe('2026-12-10T20:57:00.000Z');
  });

  it('puts the 30-minute floor exactly at 30 minutes', () => {
    expect(endOf('2026-12-09T20:30:00.000Z', '2026-12-09T21:05:00.000Z')).toBe('2026-12-09T21:00:00.000Z');
    expect(endOf('2026-12-09T20:30:00.001Z', '2026-12-09T21:05:00.000Z')).toBe('2026-12-10T09:00:00.000Z');
  });

  it('catches up a paused member one window per run, every window on the group time', () => {
    const now = '2026-12-10T22:00:00.000Z';
    const first = memberRun('2026-12-07T21:00:00.000Z', now);
    expect(first.end).toBe('2026-12-08T21:00:00.000Z');
    expect(first.body.syncGroupState).toBe(SyncGroupState.ON_GROUP_TIME);
    expect(first.body.incrementalBlocksBehind).toBe(2);
    expect(endOf(first.end, now)).toBe('2026-12-09T21:00:00.000Z');
    expect(endOf('2026-12-09T21:00:00.000Z', now)).toBe('2026-12-10T21:00:00.000Z');
  });

  it('catches up in a full window on deploy day, then realigns', () => {
    const catchUp = memberRun('2026-12-09T20:21:17.119Z', '2026-12-10T21:05:00.000Z');
    expect(catchUp.end).toBe('2026-12-10T20:21:17.119Z');
    expect(catchUp.body.syncGroupState).toBe(SyncGroupState.CATCHING_UP);
    const realign = memberRun('2026-12-10T20:21:17.119Z', '2026-12-11T05:05:00.000Z');
    expect(realign.end).toBe('2026-12-10T21:00:00.000Z');
    expect(realign.body.syncGroupState).toBe(SyncGroupState.REALIGNING);
  });

  it('realigns case prescriptions joining the group', () => {
    expect(endOf('2026-12-10T14:46:05.000Z', '2026-12-10T21:05:00.000Z')).toBe('2026-12-10T21:00:00.000Z');
  });

  it('follows a group time moved later to 21:10 with a split', () => {
    expect(endOf('2026-12-09T21:00:00.000Z', '2026-12-09T21:05:00.000Z', '21:10')).toBe('2026-12-10T09:10:00.000Z');
    expect(endOf('2026-12-10T09:10:00.000Z', '2026-12-10T21:15:00.000Z', '21:10')).toBe('2026-12-10T21:10:00.000Z');
  });

  it('keeps a 23:58 group time across the date change', () => {
    expect(endOf('2026-12-08T23:58:00.000Z', '2026-12-10T00:03:00.000Z', '23:58')).toBe('2026-12-09T23:58:00.000Z');
    expect(endOf('2026-12-09T23:58:00.000Z', '2026-12-11T00:03:00.000Z', '23:58')).toBe('2026-12-10T23:58:00.000Z');
    expect(endOf('2026-12-09T12:00:00.000Z', '2026-12-10T00:03:00.000Z', '23:58')).toBe('2026-12-09T23:58:00.000Z');
  });

  it('builds the remedy and keeps counting blocks of the window size for a member', () => {
    const { body } = memberRun('2026-12-09T20:21:17.119Z', '2026-12-10T21:05:00.000Z');
    expect(body.lastExportTime.toISOString()).toBe('2026-12-09T20:21:17.119Z');
    expect(body.incrementalExportWindowSizeInMinutes).toBe(1440);
    expect(body.incrementalBlocksBehind).toBe(0);
    expect(body.remedy).toContain(
      `ExportFromTime=${Math.floor(ms('2026-12-09T20:21:17.119Z') / 1000)},ExportToTime=${Math.floor(ms('2026-12-10T20:21:17.119Z') / 1000)},`);
  });

  it('rejects a malformed group time', () => {
    expect(() => endOf('2026-12-09T21:00:00.000Z', '2026-12-10T21:05:00.000Z', '9:00')).toThrow(/syncGroupTimeUtc/);
    expect(() => endOf('2026-12-09T21:00:00.000Z', '2026-12-10T21:05:00.000Z', '24:00')).toThrow(/syncGroupTimeUtc/);
  });

  it('accepts exactly the group times the synth accepts', () => {
    // The Lambda folder is deployed without bundling, so it keeps its own copy of the HH:MM check.
    const samples = ['00:00', '09:05', '21:00', '23:59', '9:00', '24:00', '21:60', '21:0', '2100', '', ' 21:00', '21:00 '];
    for (const time of samples) {
      const lambdaAccepts = (() => {
        try {
          endOf('2026-12-09T21:00:00.000Z', '2026-12-10T21:05:00.000Z', time);
          return true;
        } catch {
          return false;
        }
      })();
      expect([time, lambdaAccepts]).toEqual([time, isValidGroupTime(time)]);
    }
  });

  it('keeps every window between 30 minutes and 24 hours, starting at the last export time', () => {
    const dayStart = ms('2026-12-09T00:00:00.000Z');
    const nowOffsets = [1 * MINUTE, 6 * HOUR, 23 * HOUR, DAY, 30 * HOUR, 2 * DAY + 5 * MINUTE, 4 * DAY];
    for (let minute = 0; minute < 24 * 60; minute++) {
      for (const extra of [0, 412, 59_999]) {
        const start = dayStart + minute * MINUTE + extra;
        for (const offset of nowOffsets) {
          const { body } = memberRun(start, start + offset);
          const length = body.durationAddedStartTime.getTime() - start;
          expect(body.lastExportTime.getTime()).toBe(start);
          if (length < 30 * MINUTE || length > DAY) {
            throw new Error(`window of ${length} ms from ${new Date(start).toISOString()} at now+${offset}`);
          }
        }
      }
    }
  });

  it('brings every start time of day onto the group time by the next day\'s group time, through the scheduled runs', () => {
    const dayStart = ms('2026-12-09T00:00:00.000Z');
    const runOffsets = [5 * MINUTE + 5 * HOUR, 5 * MINUTE + 13 * HOUR, 5 * MINUTE + 21 * HOUR];
    const runsAfter = (start: number) => {
      const runs: number[] = [];
      for (let day = -1; day <= 4; day++) {
        for (const offset of runOffsets) {
          const run = dayStart + day * DAY + offset;
          if (run > start) runs.push(run);
        }
      }
      return runs.sort((a, b) => a - b);
    };

    const starts: number[] = [];
    for (let minute = 0; minute < 24 * 60; minute++) starts.push(dayStart + minute * MINUTE);
    for (const offset of runOffsets) starts.push(dayStart + offset + 1_000, dayStart + offset + 3_000);

    for (const start of starts) {
      let lastExport = start;
      let alignedAt: number | undefined;
      for (const run of runsAfter(start)) {
        const { body } = memberRun(lastExport, run);
        const end = body.durationAddedStartTime.getTime();
        if (end > run) continue; // the existing check: export not needed yet
        expect(body.lastExportTime.getTime()).toBe(lastExport); // each window starts where the previous one ended
        lastExport = end;
        if (new Date(end).toISOString().endsWith('T21:00:00.000Z')) {
          alignedAt = end;
          break;
        }
      }
      if (alignedAt === undefined || alignedAt - start > DAY + 30 * MINUTE) {
        throw new Error(`start ${new Date(start).toISOString()} reached the group time at ${alignedAt && new Date(alignedAt).toISOString()}`);
      }
    }
  });
});

describe('without a group time', () => {
  it.each([15, 1440])('returns the unchanged body for a %i-minute window', (windowMinutes) => {
    const cases: Array<[string, string]> = [
      ['2026-11-19T20:16:56.306Z', '2026-11-20T20:16:55.000Z'],
      ['2026-11-19T20:16:56.306Z', '2026-11-21T03:00:00.000Z'],
      ['2026-11-09T20:21:17.000Z', '2026-11-19T02:10:00.000Z'],
      ['2026-11-19T19:43:55.000Z', '2026-11-19T19:44:00.000Z'],
    ];
    for (const [lastExportTime, now] of cases) {
      for (const bucketPrefix of ['ddb/appointments', '']) {
        const event = { ...baseEvent, bucketPrefix, incrementalExportWindowSizeInMinutes: windowMinutes, lastExportTime };
        const { body } = computeNextWindow(event, new Date(now));
        expect(body).toEqual(legacyBody(event, new Date(now)));
        expect(Object.keys(body).sort()).toEqual(
          ['durationAddedStartTime', 'incrementalBlocksBehind', 'incrementalExportWindowSizeInMinutes', 'lastExportTime', 'remedy']);
      }
    }
  });
});

describe('index.mjs handler', () => {
  /** Imports the deployed ES module in a separate Node process and calls its handler. */
  function invokeHandler(event: Record<string, unknown>) {
    const url = pathToFileURL(path.join(lambdaDir, 'index.mjs')).href;
    const script = [
      'const logs = [];',
      "console.log = (...args) => logs.push(args.map(a => typeof a === 'string' ? a : JSON.stringify(a)).join(' '));",
      `const { handler } = await import(${JSON.stringify(url)});`,
      `const response = await handler(${JSON.stringify(event)}, {});`,
      'process.stdout.write(JSON.stringify({ response, logs }));',
    ].join('\n');
    const output = execFileSync(process.execPath, ['--input-type=module', '-e', script], { encoding: 'utf8' });
    return JSON.parse(output) as { response: { statusCode: number; body: Record<string, unknown> }; logs: string[] };
  }

  it('loads as an ES module and returns the unchanged window for a non-member', () => {
    const { response, logs } = invokeHandler({ ...baseEvent, lastExportTime: '2026-11-19T20:16:56.306Z' });
    expect(response.statusCode).toBe(200);
    expect(response.body.lastExportTime).toBe('2026-11-19T20:16:56.306Z');
    expect(response.body.durationAddedStartTime).toBe('2026-11-20T20:16:56.306Z');
    expect(response.body).not.toHaveProperty('syncGroupState');
    expect(logs.some(line => line.includes('Incremental blocks behind:'))).toBe(true);
    expect(logs.some(line => line.includes('Sync group'))).toBe(false);
  });

  it('logs the group time, the window and the state for a member', () => {
    // Three days behind, at a millisecond that is never a group time: the member is catching up.
    const start = Math.floor((Date.now() - 3 * DAY) / 1000) * 1000 + 123;
    const lastExportTime = new Date(start).toISOString();
    const { response, logs } = invokeHandler({ ...baseEvent, lastExportTime, syncGroupTimeUtc: '21:00' });
    expect(response.body.lastExportTime).toBe(lastExportTime);
    expect(response.body.durationAddedStartTime).toBe(new Date(start + DAY).toISOString());
    expect(response.body.syncGroupState).toBe(SyncGroupState.CATCHING_UP);
    const line = logs.find(entry => entry.includes('Sync group time 21:00 UTC'));
    expect(line).toBeDefined();
    expect(line).toContain(`window ${lastExportTime} to ${response.body.durationAddedStartTime}`);
    expect(line).toContain(SyncGroupState.CATCHING_UP);
  });
});
