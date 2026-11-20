import * as fs from 'fs';
import * as cdk from 'aws-cdk-lib';
import { IConstruct } from 'constructs';
import { isTableDisabled, normalizeTableName, tableMatchesEntry } from './configuration';

/** Every member's windows end at the group time once a day, so a group needs one-day windows. */
export const SYNC_GROUP_WINDOW_SIZE_IN_MINUTES = 1440;

/** Member tables whose daily export windows all end at one time. */
export interface SyncGroup {
  /** Names the group in errors and warnings. */
  name: string;
  /** Daily window end, two-digit `HH:MM` in UTC. */
  timeUtc: string;
  /** Table entries, matched like `disabledTables`: full table name or last name segment, ignoring case. */
  members: string[];
}

/** `config/sync-groups.json`: the groups of each environment, keyed `<ENVIRONMENT_SHORT>-<EHAWK_REGION>`. */
export type SyncGroupsConfig = Record<string, SyncGroup[]>;

const GROUP_TIME_PATTERN = /^([01]\d|2[0-3]):[0-5]\d$/;
const ENVIRONMENT_KEY_PATTERN = /^[a-z0-9]+-[a-z0-9]+$/;

/** True for a two-digit `HH:MM` between 00:00 and 23:59. */
export function isValidGroupTime(value: unknown): value is string {
  return typeof value === 'string' && GROUP_TIME_PATTERN.test(value);
}

/** Reads and validates a sync groups file; throws when it is missing, not JSON, or invalid. */
export function loadSyncGroupsConfig(filePath: string): SyncGroupsConfig {
  let raw: unknown;
  try {
    raw = JSON.parse(fs.readFileSync(filePath, 'utf8').replace(/^\uFEFF/, ''));
  } catch (error) {
    throw new Error(`Cannot read sync groups configuration ${filePath}: ${(error as Error).message}`);
  }
  try {
    return validateSyncGroupsConfig(raw);
  } catch (error) {
    throw new Error(`${filePath}: ${(error as Error).message}`);
  }
}

/**
 * Checks the parsed file and returns it typed, with names and members trimmed. Throws one error listing
 * every problem: a malformed shape or environment key, a time that is not a two-digit `HH:MM` within
 * 00:00-23:59, two groups with one name, or one table named twice within an environment.
 */
export function validateSyncGroupsConfig(raw: unknown): SyncGroupsConfig {
  if (!isPlainObject(raw)) {
    throw invalidConfig(['the file must be a JSON object keyed by <ENVIRONMENT_SHORT>-<EHAWK_REGION>']);
  }
  const errors: string[] = [];
  const config: SyncGroupsConfig = {};
  for (const [environmentKey, groups] of Object.entries(raw)) {
    if (!ENVIRONMENT_KEY_PATTERN.test(environmentKey)) {
      errors.push(`environment key '${environmentKey}' is not a lowercase <ENVIRONMENT_SHORT>-<EHAWK_REGION>`);
      continue;
    }
    if (!Array.isArray(groups)) {
      errors.push(`${environmentKey} must be a list of groups`);
      continue;
    }
    config[environmentKey] = validateEnvironmentGroups(environmentKey, groups, errors);
  }
  if (errors.length > 0) {
    throw invalidConfig(errors);
  }
  return config;
}

function validateEnvironmentGroups(environmentKey: string, groups: unknown[], errors: string[]): SyncGroup[] {
  const result: SyncGroup[] = [];
  const groupNames = new Set<string>();
  // Normalized member -> where it was first named. Matching is by normalized name, so equal names match the same tables.
  const namedMembers = new Map<string, { groupIndex: number; label: string; member: string }>();

  groups.forEach((rawGroup, groupIndex) => {
    const position = `${environmentKey} group ${groupIndex + 1}`;
    if (!isPlainObject(rawGroup)) {
      errors.push(`${position} must be an object`);
      return;
    }

    const name = typeof rawGroup.name === 'string' ? rawGroup.name.trim() : '';
    if (name === '') {
      errors.push(`${position} needs a non-empty name`);
    } else if (groupNames.has(name)) {
      errors.push(`${environmentKey} has two groups named '${name}'`);
    }
    groupNames.add(name);
    const label = name === '' ? position : `${environmentKey} group '${name}'`;

    if (!isValidGroupTime(rawGroup.timeUtc)) {
      errors.push(`${label} time ${JSON.stringify(rawGroup.timeUtc)} is not a two-digit HH:MM UTC time within 00:00-23:59`);
    }

    const members: string[] = [];
    if (!Array.isArray(rawGroup.members) || rawGroup.members.length === 0) {
      errors.push(`${label} needs a non-empty members list`);
    } else {
      for (const rawMember of rawGroup.members) {
        if (typeof rawMember !== 'string' || rawMember.trim() === '') {
          errors.push(`${label} has a member that is not a non-empty string: ${JSON.stringify(rawMember)}`);
          continue;
        }
        const member = rawMember.trim();
        const first = namedMembers.get(normalizeTableName(member));
        if (first === undefined) {
          namedMembers.set(normalizeTableName(member), { groupIndex, label, member });
        } else if (first.groupIndex === groupIndex) {
          errors.push(`${label} names one table twice: '${first.member}' and '${member}'`);
        } else {
          errors.push(`'${first.member}' (${first.label}) and '${member}' (${label}) put one table in two groups; ` +
            'a table belongs to at most one sync group');
        }
        members.push(member);
      }
    }

    result.push({ name, timeUtc: String(rawGroup.timeUtc), members });
  });
  return result;
}

/** `<ENVIRONMENT_SHORT>-<EHAWK_REGION>` (lowercase), or undefined when either variable is missing or blank. */
export function syncGroupEnvironmentKey(env: Readonly<Record<string, string | undefined>>): string | undefined {
  const environmentShort = (env.ENVIRONMENT_SHORT ?? '').trim().toLowerCase();
  const region = (env.EHAWK_REGION ?? '').trim().toLowerCase();
  return environmentShort === '' || region === '' ? undefined : `${environmentShort}-${region}`;
}

export interface SyncGroupResolutionInput {
  config: SyncGroupsConfig;
  /** From syncGroupEnvironmentKey; undefined or a key absent from the config means no group. */
  environmentKey: string | undefined;
  /** The tables this deploy exports, as bin/ names their nested stacks. */
  tableNames: string[];
  incrementalExportWindowSizeInMinutes: number;
  disabledTables?: string[];
}

export interface SyncGroupResolution {
  /** Group time (`HH:MM` UTC) of each deployed table in a group; tables outside every group are absent. */
  groupTimeByTable: Map<string, string>;
  /** For addSyncGroupWarnings on the shared stack. */
  warnings: string[];
}

interface MemberMatch {
  group: SyncGroup;
  entry: string;
  tables: string[];
}

/**
 * Gives each deployed table its group time. Throws, failing the synth, when a member entry matches more
 * than one deployed table, when two entries match one table, or when a deployed table is in a group and
 * the window is not 1440 minutes. Warns when only some of a group's members are deployed (none is silent:
 * a deploy that exports none of the members, such as ehawk-repath-places) and when a deployed member's
 * schedule is disabled by `disabledTables`.
 */
export function resolveSyncGroups(input: SyncGroupResolutionInput): SyncGroupResolution {
  const { config, environmentKey, tableNames, incrementalExportWindowSizeInMinutes } = input;
  const groups = environmentKey !== undefined && Object.prototype.hasOwnProperty.call(config, environmentKey)
    ? config[environmentKey]
    : [];

  const matches: MemberMatch[] = groups.flatMap(group => group.members.map(entry => ({
    group,
    entry,
    tables: tableNames.filter(tableName => tableMatchesEntry(tableName, entry)),
  })));

  for (const match of matches) {
    if (match.tables.length > 1) {
      throw new Error(`Sync group '${match.group.name}' member '${match.entry}' matches more than one deployed table: ` +
        match.tables.join(', '));
    }
  }

  const matchByTable = new Map<string, MemberMatch>();
  for (const match of matches) {
    if (match.tables.length === 0) {
      continue;
    }
    const tableName = match.tables[0];
    const previous = matchByTable.get(tableName);
    if (previous !== undefined) {
      throw new Error(`Table '${tableName}' matches more than one sync group entry: ` +
        `'${previous.entry}' (group '${previous.group.name}') and '${match.entry}' (group '${match.group.name}')`);
    }
    matchByTable.set(tableName, match);
  }

  if (matchByTable.size > 0 && incrementalExportWindowSizeInMinutes !== SYNC_GROUP_WINDOW_SIZE_IN_MINUTES) {
    throw new Error(`Sync groups need incrementalExportWindowSizeInMinutes ${SYNC_GROUP_WINDOW_SIZE_IN_MINUTES}, ` +
      `but it is ${incrementalExportWindowSizeInMinutes} in ${environmentKey}, where these deployed tables are in a group: ` +
      [...matchByTable.keys()].join(', '));
  }

  const groupTimeByTable = new Map<string, string>();
  for (const [tableName, match] of matchByTable) {
    groupTimeByTable.set(tableName, match.group.timeUtc);
  }

  const warnings: string[] = [];
  for (const group of groups) {
    const groupMatches = matches.filter(match => match.group === group);
    const unmatched = groupMatches.filter(match => match.tables.length === 0).map(match => `'${match.entry}'`);
    if (unmatched.length > 0 && unmatched.length < groupMatches.length) {
      const subject = unmatched.length === 1 ? `member ${unmatched[0]} matches` : `members ${unmatched.join(', ')} match`;
      warnings.push(`Sync group '${group.name}' (${environmentKey}): ${subject} no table being exported, ` +
        `so only the other members' windows end at ${group.timeUtc} UTC`);
    }
  }
  for (const [tableName, match] of matchByTable) {
    if (isTableDisabled(tableName, input.disabledTables)) {
      warnings.push(`Table '${tableName}' is in sync group '${match.group.name}' (${environmentKey}) ` +
        'but its schedule is disabled by disabledTables, so it starts no exports at the group time');
    }
  }

  return { groupTimeByTable, warnings };
}

/**
 * Attaches the warnings to a stack. Pass the shared `${stackName}-export` stack: CDK leaves annotations
 * on the App out of the cloud assembly, so App-level warnings never surface.
 */
export function addSyncGroupWarnings(scope: IConstruct, warnings: string[]): void {
  warnings.forEach(warning => cdk.Annotations.of(scope).addWarning(warning));
}

/**
 * Tuning: a member runs this long after each group time. DynamoDB accepted window ends a fraction of a
 * second old, and EventBridge Scheduler fires within a minute with the flexible window off.
 */
export const SYNC_GROUP_RUN_OFFSET_IN_MINUTES = 5;

const MINUTES_PER_DAY = 24 * 60;

/** A member's run times, minutes after midnight UTC: the offset after the group time, then every third of the window. */
function memberRunMinutes(groupTimeUtc: string, incrementalExportWindowSizeInMinutes: number): number[] {
  if (!isValidGroupTime(groupTimeUtc)) {
    throw new Error(`Sync group time '${groupTimeUtc}' is not a two-digit HH:MM UTC time within 00:00-23:59`);
  }
  if (incrementalExportWindowSizeInMinutes !== SYNC_GROUP_WINDOW_SIZE_IN_MINUTES) {
    throw new Error(`Sync group schedules need incrementalExportWindowSizeInMinutes ${SYNC_GROUP_WINDOW_SIZE_IN_MINUTES}, ` +
      `not ${incrementalExportWindowSizeInMinutes}`);
  }
  const [hours, minutes] = groupTimeUtc.split(':').map(Number);
  const firstRun = hours * 60 + minutes + SYNC_GROUP_RUN_OFFSET_IN_MINUTES;
  const interval = incrementalExportWindowSizeInMinutes / 3;
  return [0, 1, 2].map(run => (firstRun + run * interval) % MINUTES_PER_DAY);
}

/**
 * EventBridge Scheduler cron expression, in UTC, for a member's one schedule. Three runs a day keep a
 * member that is behind exporting one window per run, as the 8-hourly rate does for other tables.
 */
export function syncGroupScheduleExpression(groupTimeUtc: string, incrementalExportWindowSizeInMinutes: number): string {
  const runs = memberRunMinutes(groupTimeUtc, incrementalExportWindowSizeInMinutes);
  // The runs are whole hours apart, so they share the minute.
  const hours = runs.map(run => Math.floor(run / 60)).sort((a, b) => a - b);
  return `cron(${runs[0] % 60} ${hours.join(',')} * * ? *)`;
}

/** Schedule description naming a member's run times and its group time. */
export function syncGroupScheduleDescription(groupTimeUtc: string, incrementalExportWindowSizeInMinutes: number): string {
  const runs = memberRunMinutes(groupTimeUtc, incrementalExportWindowSizeInMinutes).map(formatMinutes);
  return `Triggers the step function at ${runs[0]}, ${runs[1]} and ${runs[2]} UTC, ` +
    `${SYNC_GROUP_RUN_OFFSET_IN_MINUTES} minutes after sync group time ${groupTimeUtc} UTC`;
}

function formatMinutes(minutesOfDay: number): string {
  const pad = (value: number) => String(value).padStart(2, '0');
  return `${pad(Math.floor(minutesOfDay / 60))}:${pad(minutesOfDay % 60)}`;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function invalidConfig(problems: string[]): Error {
  return new Error(`Invalid sync groups configuration:\n${problems.map(problem => `  - ${problem}`).join('\n')}`);
}
