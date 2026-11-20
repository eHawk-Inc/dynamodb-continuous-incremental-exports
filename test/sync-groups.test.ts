import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Annotations, Match } from 'aws-cdk-lib/assertions';
import { DynamoDbContinuousIncrementalSharedResourceStack } from '../lib/dynamodb-continuous-incremental-shared-resources-stack';
import { Configuration } from '../lib/configuration';
import { ExportFormat } from '../lib/constants/exportFormat';
import { ExportViewType } from '../lib/constants/exportViewType';
import {
  SyncGroupsConfig,
  addSyncGroupWarnings,
  isValidGroupTime,
  loadSyncGroupsConfig,
  resolveSyncGroups,
  syncGroupEnvironmentKey,
  validateSyncGroupsConfig,
} from '../lib/syncGroups';

const COMMITTED_FILE = path.join(__dirname, '..', 'config', 'sync-groups.json');

const CASE_EPISODE = 'ehawk-mobilehub-1778525198-CaseEpisode';
const APPOINTMENTS = 'ehawk-mobilehub-1778525198-Appointments';
const CASE_PRESCRIPTION = 'ehawk-mobilehub-1778525198-CasePrescription';
const CASE = 'ehawk-mobilehub-1778525198-Case';
const ROUTE_ENTRY = 'ehawk-mobilehub-1778525198-RouteEntry';

const DEPLOYED_TABLES = [CASE_EPISODE, APPOINTMENTS, CASE_PRESCRIPTION, CASE, ROUTE_ENTRY];
// The ehawk-repath-places deploy builds this exporter with the same environment keys but none of the members.
const PLACES_TABLES = ['ehawk-repath-places-Visit', 'ehawk-repath-places-Transit', 'ehawk-repath-places-GroupedLocation'];

function groupTimesFor(
  config: SyncGroupsConfig,
  environmentKey: string | undefined,
  tableNames: string[] = DEPLOYED_TABLES,
  incrementalExportWindowSizeInMinutes = 1440,
  disabledTables: string[] = [],
) {
  return resolveSyncGroups({ config, environmentKey, tableNames, incrementalExportWindowSizeInMinutes, disabledTables });
}

function group(name: string, timeUtc: string, members: string[]) {
  return { name, timeUtc, members };
}

describe('committed config/sync-groups.json', () => {
  const committed = loadSyncGroupsConfig(COMMITTED_FILE);

  it.each(['prd-us', 'stg-us'])('resolves the three members to 21:00 under %s', environmentKey => {
    const { groupTimeByTable } = groupTimesFor(committed, environmentKey);
    expect(groupTimeByTable.get(CASE_EPISODE)).toBe('21:00');
    expect(groupTimeByTable.get(APPOINTMENTS)).toBe('21:00');
    expect(groupTimeByTable.get(CASE_PRESCRIPTION)).toBe('21:00');
  });

  it('leaves a non-member without a group under prd-us', () => {
    const { groupTimeByTable } = groupTimesFor(committed, 'prd-us');
    expect(groupTimeByTable.has(CASE)).toBe(false);
    expect(groupTimeByTable.get(CASE)).toBeUndefined();
    expect(groupTimeByTable.has(ROUTE_ENTRY)).toBe(false);
    expect(groupTimeByTable.size).toBe(3);
  });

  it.each([
    ['dev-us', 1440],
    ['prd-ca', 4320],
    ['stg-ca', 4320],
  ])('resolves every table to no group under %s (window %d)', (environmentKey, windowSize) => {
    const resolution = groupTimesFor(committed, environmentKey, DEPLOYED_TABLES, windowSize);
    expect(resolution.groupTimeByTable.size).toBe(0);
    expect(resolution.warnings).toEqual([]);
  });

  it('has no group outside US Production and US Staging', () => {
    expect(Object.keys(committed).sort()).toEqual(['prd-us', 'stg-us']);
  });

  it('yields no warning for the places-style deploy that holds none of the members', () => {
    for (const environmentKey of ['prd-us', 'stg-us']) {
      const resolution = groupTimesFor(committed, environmentKey, PLACES_TABLES);
      expect(resolution.groupTimeByTable.size).toBe(0);
      expect(resolution.warnings).toEqual([]);
    }
  });

  it('warns when only two of the three members are deployed', () => {
    const resolution = groupTimesFor(committed, 'prd-us', [CASE_EPISODE, APPOINTMENTS, CASE]);
    expect(resolution.groupTimeByTable.get(CASE_EPISODE)).toBe('21:00');
    expect(resolution.groupTimeByTable.get(APPOINTMENTS)).toBe('21:00');
    expect(resolution.warnings).toHaveLength(1);
    expect(resolution.warnings[0]).toMatch(/CasePrescription/);
    expect(resolution.warnings[0]).toMatch(/match(es)? no table being exported/);
  });
});

describe('syncGroupEnvironmentKey', () => {
  it('joins ENVIRONMENT_SHORT and EHAWK_REGION', () => {
    expect(syncGroupEnvironmentKey({ ENVIRONMENT_SHORT: 'prd', EHAWK_REGION: 'us' })).toBe('prd-us');
    expect(syncGroupEnvironmentKey({ ENVIRONMENT_SHORT: ' stg ', EHAWK_REGION: 'CA' })).toBe('stg-ca');
  });

  it('gives no key when either variable is missing or blank', () => {
    expect(syncGroupEnvironmentKey({ EHAWK_REGION: 'us' })).toBeUndefined();
    expect(syncGroupEnvironmentKey({ ENVIRONMENT_SHORT: 'prd' })).toBeUndefined();
    expect(syncGroupEnvironmentKey({ ENVIRONMENT_SHORT: '', EHAWK_REGION: 'us' })).toBeUndefined();
    expect(syncGroupEnvironmentKey({ ENVIRONMENT_SHORT: 'prd', EHAWK_REGION: '  ' })).toBeUndefined();
    expect(syncGroupEnvironmentKey({})).toBeUndefined();
  });

  it('resolves every table to no group when a variable is missing', () => {
    const committed = loadSyncGroupsConfig(COMMITTED_FILE);
    for (const env of [{ EHAWK_REGION: 'us' }, { ENVIRONMENT_SHORT: 'prd' }]) {
      const resolution = groupTimesFor(committed, syncGroupEnvironmentKey(env));
      expect(resolution.groupTimeByTable.size).toBe(0);
      expect(resolution.warnings).toEqual([]);
    }
  });
});

describe('resolveSyncGroups', () => {
  it('matches ignoring case and accepts the full table name as the entry', () => {
    const config = validateSyncGroupsConfig({
      'prd-us': [group('g', '21:00', ['caseepisode', 'CASEPRESCRIPTION', APPOINTMENTS])],
    });
    const { groupTimeByTable } = groupTimesFor(config, 'prd-us');
    expect(groupTimeByTable.get(CASE_EPISODE)).toBe('21:00');
    expect(groupTimeByTable.get(CASE_PRESCRIPTION)).toBe('21:00');
    expect(groupTimeByTable.get(APPOINTMENTS)).toBe('21:00');
    expect(groupTimeByTable.has(CASE)).toBe(false);
  });

  it('switches a group on in another 1440-minute environment through configuration alone', () => {
    const config = validateSyncGroupsConfig({
      'dev-us': [group('dev-group', '15:00', ['CaseEpisode', 'Appointments', 'CasePrescription'])],
    });
    const dev = groupTimesFor(config, 'dev-us');
    expect(dev.groupTimeByTable.get(CASE_EPISODE)).toBe('15:00');
    expect(dev.groupTimeByTable.get(APPOINTMENTS)).toBe('15:00');
    expect(dev.groupTimeByTable.get(CASE_PRESCRIPTION)).toBe('15:00');
    expect(groupTimesFor(config, 'prd-us').groupTimeByTable.size).toBe(0);
  });

  it('gives each group its own time', () => {
    const config = validateSyncGroupsConfig({
      'prd-us': [group('a', '21:00', ['CaseEpisode']), group('b', '03:30', ['RouteEntry'])],
    });
    const { groupTimeByTable } = groupTimesFor(config, 'prd-us', [CASE_EPISODE, ROUTE_ENTRY, CASE]);
    expect(groupTimeByTable.get(CASE_EPISODE)).toBe('21:00');
    expect(groupTimeByTable.get(ROUTE_ENTRY)).toBe('03:30');
    expect(groupTimeByTable.has(CASE)).toBe(false);
  });

  it('gives no group for an unknown environment key or no key', () => {
    const config = validateSyncGroupsConfig({ 'prd-us': [group('g', '21:00', ['CaseEpisode'])] });
    expect(groupTimesFor(config, 'qa-us').groupTimeByTable.size).toBe(0);
    expect(groupTimesFor(config, undefined).groupTimeByTable.size).toBe(0);
  });

  it('fails when a member entry matches two deployed tables', () => {
    const config = validateSyncGroupsConfig({ 'prd-us': [group('g', '21:00', ['CaseEpisode'])] });
    expect(() => groupTimesFor(config, 'prd-us', [CASE_EPISODE, 'chat_CaseEpisode']))
      .toThrow(/'CaseEpisode'.*matches more than one/);
  });

  it('fails when two entries match the same deployed table', () => {
    // Bypasses validation, which already rejects the duplicate; the resolver still refuses it.
    const config: SyncGroupsConfig = {
      'prd-us': [group('g', '21:00', ['CaseEpisode', CASE_EPISODE])],
    };
    expect(() => groupTimesFor(config, 'prd-us', [CASE_EPISODE])).toThrow(/matches more than one sync group entry/);
  });

  it('fails when a deployed member has a window other than 1440 minutes', () => {
    const config = validateSyncGroupsConfig({ 'prd-us': [group('g', '21:00', ['CaseEpisode'])] });
    expect(() => groupTimesFor(config, 'prd-us', [CASE_EPISODE, CASE], 720)).toThrow(/1440/);
  });

  it('does not fail on a 720-minute window when no member is deployed', () => {
    const config = validateSyncGroupsConfig({ 'prd-us': [group('g', '21:00', ['CaseEpisode'])] });
    const resolution = groupTimesFor(config, 'prd-us', [CASE, ROUTE_ENTRY], 720);
    expect(resolution.groupTimeByTable.size).toBe(0);
    expect(resolution.warnings).toEqual([]);
  });

  it('warns when a deployed member is also in disabledTables', () => {
    const config = validateSyncGroupsConfig({
      'prd-us': [group('g', '21:00', ['CaseEpisode', 'Appointments', 'CasePrescription'])],
    });
    const resolution = groupTimesFor(config, 'prd-us', DEPLOYED_TABLES, 1440, ['Appointments', 'RouteEntry']);
    expect(resolution.groupTimeByTable.get(APPOINTMENTS)).toBe('21:00');
    expect(resolution.warnings).toHaveLength(1);
    expect(resolution.warnings[0]).toMatch(new RegExp(`${APPOINTMENTS}.*disabledTables`));
  });
});

describe('validateSyncGroupsConfig', () => {
  it('accepts two-digit times from 00:00 to 23:59', () => {
    for (const time of ['00:00', '09:05', '21:00', '23:59']) {
      expect(isValidGroupTime(time)).toBe(true);
    }
  });

  it.each(['21:60', '24:00', '9:00', '21:0', '2100', '', ' 21:00'])('rejects the group time %p', time => {
    expect(isValidGroupTime(time)).toBe(false);
    expect(() => validateSyncGroupsConfig({ 'prd-us': [group('g', time, ['CaseEpisode'])] })).toThrow(/time/i);
  });

  it('rejects a member named in two groups of the same environment', () => {
    expect(() => validateSyncGroupsConfig({
      'prd-us': [group('a', '21:00', ['CaseEpisode']), group('b', '03:00', [CASE_EPISODE])],
    })).toThrow(/CaseEpisode.*two groups|two groups.*CaseEpisode/);
  });

  it('rejects a member named twice in one group', () => {
    expect(() => validateSyncGroupsConfig({
      'prd-us': [group('a', '21:00', ['CaseEpisode', 'caseepisode'])],
    })).toThrow(/twice/);
  });

  it('allows the same member in groups of different environments', () => {
    const config = validateSyncGroupsConfig({
      'prd-us': [group('a', '21:00', ['CaseEpisode'])],
      'stg-us': [group('a', '21:00', ['CaseEpisode'])],
    });
    expect(Object.keys(config)).toEqual(['prd-us', 'stg-us']);
  });

  it.each([
    ['a non-object file', []],
    ['a null file', null],
    ['an environment that is not a list', { 'prd-us': {} }],
    ['a malformed environment key', { 'PRD_US': [group('a', '21:00', ['CaseEpisode'])] }],
    ['a group without a name', { 'prd-us': [{ timeUtc: '21:00', members: ['CaseEpisode'] }] }],
    ['a group without members', { 'prd-us': [group('a', '21:00', [])] }],
    ['a blank member', { 'prd-us': [group('a', '21:00', ['  '])] }],
    ['a non-string member', { 'prd-us': [{ name: 'a', timeUtc: '21:00', members: [42] }] }],
    ['a group time that is not a string', { 'prd-us': [{ name: 'a', timeUtc: 2100, members: ['CaseEpisode'] }] }],
    ['two groups with the same name', { 'prd-us': [group('a', '21:00', ['CaseEpisode']), group('a', '03:00', ['Case'])] }],
  ])('rejects %s', (_description, raw) => {
    expect(() => validateSyncGroupsConfig(raw)).toThrow(/sync groups/i);
  });
});

describe('addSyncGroupWarnings', () => {
  const configuration: Configuration = {
    environmentName: 'test',
    sourceDynamoDbTableName: DEPLOYED_TABLES.join(','),
    deploymentAlias: 'test-alias',
    failureNotificationEmail: '',
    successNotificationEmail: '',
    dataExportBucketName: 'bucket',
    dataExportBucketPrefix: 'prefix',
    incrementalExportWindowSizeInMinutes: 1440,
    waitTimeToCheckExportStatusInSeconds: 10,
    exportViewType: ExportViewType.NEW_AND_OLD_IMAGES,
    exportFormat: ExportFormat.DYNAMODB_JSON,
    awsApiInvocationTimeoutInSeconds: 30,
    disabledTables: ['CaseEpisode'],
  };

  it('puts the partial-match and disabled-member warnings on the shared stack', () => {
    const app = new cdk.App();
    const sharedStack = new DynamoDbContinuousIncrementalSharedResourceStack(app, 'test-export', {
      configuration,
      useExistingNotificationTopic: false,
    });
    const committed = loadSyncGroupsConfig(COMMITTED_FILE);
    const resolution = groupTimesFor(committed, 'prd-us', [CASE_EPISODE, APPOINTMENTS, CASE], 1440, ['CaseEpisode']);
    expect(resolution.warnings).toHaveLength(2);

    addSyncGroupWarnings(sharedStack, resolution.warnings);

    const annotations = Annotations.fromStack(sharedStack);
    annotations.hasWarning('*', Match.stringLikeRegexp('CasePrescription.*match(es)? no table being exported'));
    annotations.hasWarning('*', Match.stringLikeRegexp(`${CASE_EPISODE}.*disabledTables`));
  });

  it('adds nothing when there are no warnings', () => {
    const app = new cdk.App();
    const sharedStack = new DynamoDbContinuousIncrementalSharedResourceStack(app, 'test-export', {
      configuration,
      useExistingNotificationTopic: false,
    });
    addSyncGroupWarnings(sharedStack, []);
    Annotations.fromStack(sharedStack).hasNoWarning('*', Match.stringLikeRegexp('[Ss]ync group'));
  });
});
