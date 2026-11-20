import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { Configuration } from '../lib/configuration';
import { ExportFormat } from '../lib/constants/exportFormat';
import { ExportViewType } from '../lib/constants/exportViewType';
import { DynamoDbContinuousIncrementalExportsStack } from '../lib/dynamodb-continuous-incremental-exports-stack';
import { DynamoDbContinuousIncrementalSharedResourceStack } from '../lib/dynamodb-continuous-incremental-shared-resources-stack';
import { syncGroupScheduleExpression } from '../lib/syncGroups';

const baseConfiguration: Configuration = {
  environmentName: 'stage-us',
  sourceDynamoDbTableName: '',
  deploymentAlias: 'stg-us-dl-ingest',
  failureNotificationEmail: '',
  successNotificationEmail: '',
  dataExportBucketOwnerAccountId: '111111111111',
  dataExportBucketName: 'example-trn-bucket',
  dataExportBucketPrefix: 'product/repath',
  incrementalExportWindowSizeInMinutes: 1440,
  waitTimeToCheckExportStatusInSeconds: 10,
  exportViewType: ExportViewType.NEW_AND_OLD_IMAGES,
  exportFormat: ExportFormat.DYNAMODB_JSON,
  awsApiInvocationTimeoutInSeconds: 10,
  disabledTables: [],
};

/** Synthesizes one table's nested stack the way bin/ builds it. */
async function synthTable(tableName: string, overrides: Partial<Configuration> = {}) {
  const app = new cdk.App();
  const shared = new DynamoDbContinuousIncrementalSharedResourceStack(app, 'test-export', {
    configuration: baseConfiguration,
    useExistingNotificationTopic: false,
  });
  const stack = new DynamoDbContinuousIncrementalExportsStack(shared, tableName, {
    kmsKeyUsedForSnsTopic: shared.kmsKeyUsedForSnsTopic,
    ddbExportNotificationTopic: shared.ddbExportNotificationTopic,
    schedulerRole: shared.schedulerRole,
    configuration: {
      ...baseConfiguration,
      deploymentAlias: `${baseConfiguration.deploymentAlias}-${tableName.split('-').pop()!.toLowerCase()}`,
      sourceDynamoDbTableName: tableName,
      ...overrides,
    },
  });
  // The stack builds its resources after an await in init; let it settle before synthesizing.
  await new Promise(resolve => setImmediate(resolve));
  return Template.fromStack(stack);
}

function scheduleOf(template: Template) {
  const schedules = Object.values(template.findResources('AWS::Scheduler::Schedule'));
  expect(schedules).toHaveLength(1);
  return schedules[0].Properties;
}

/** The state machine definition with CloudFormation tokens replaced by a placeholder. */
function definitionOf(template: Template) {
  const machines = Object.values(template.findResources('AWS::StepFunctions::StateMachine'));
  expect(machines).toHaveLength(1);
  const definition = machines[0].Properties.DefinitionString;
  const text = typeof definition === 'string'
    ? definition
    : (definition['Fn::Join'][1] as unknown[]).map(part => (typeof part === 'string' ? part : '<token>')).join('');
  return JSON.parse(text);
}

const nextWindowPayload = (template: Template) =>
  definitionOf(template).States.GetNextIncrementalExportTime.Parameters.Payload;

describe('sync group schedule expression', () => {
  it.each([
    ['21:00', 'cron(5 5,13,21 * * ? *)'],
    ['23:58', 'cron(3 0,8,16 * * ? *)'],
    ['15:00', 'cron(5 7,15,23 * * ? *)'],
    ['00:00', 'cron(5 0,8,16 * * ? *)'],
    ['18:55', 'cron(0 3,11,19 * * ? *)'],
  ])('runs 5 minutes after %s and every 8 hours after', (groupTime, expression) => {
    expect(syncGroupScheduleExpression(groupTime, 1440)).toBe(expression);
  });

  it('rejects a window that is not 1440 minutes', () => {
    expect(() => syncGroupScheduleExpression('21:00', 720)).toThrow(/1440/);
  });
});

describe('per-table stack', () => {
  it('gives a member the fixed-time schedule in UTC with the flexible window off', async () => {
    const schedule = scheduleOf(await synthTable('ehawk-mobilehub-1778525198-CaseEpisode', { syncGroupTimeUtc: '21:00' }));
    expect(schedule.ScheduleExpression).toBe('cron(5 5,13,21 * * ? *)');
    expect(schedule.ScheduleExpressionTimezone).toBe('UTC');
    expect(schedule.FlexibleTimeWindow).toEqual({ Mode: 'OFF' });
    expect(schedule.State).toBe('ENABLED');
    expect(schedule.Description).toBe('Triggers the step function at 21:05, 05:05 and 13:05 UTC, 5 minutes after sync group time 21:00 UTC');
    expect(schedule.Target.RetryPolicy).toEqual({ MaximumEventAgeInSeconds: 14400, MaximumRetryAttempts: 1 });
  });

  it('gives a 23:58 member a schedule that wraps past midnight', async () => {
    const schedule = scheduleOf(await synthTable('ehawk-mobilehub-1778525198-Appointments', { syncGroupTimeUtc: '23:58' }));
    expect(schedule.ScheduleExpression).toBe('cron(3 0,8,16 * * ? *)');
  });

  it('gives a dev member at 15:00 its own schedule', async () => {
    const schedule = scheduleOf(await synthTable('ehawk-mobilehub-1778525198-CasePrescription', { syncGroupTimeUtc: '15:00' }));
    expect(schedule.ScheduleExpression).toBe('cron(5 7,15,23 * * ? *)');
  });

  it('keeps a non-member on its 8-hourly rate with the 15-minute flexible window', async () => {
    const schedule = scheduleOf(await synthTable('ehawk-mobilehub-1778525198-Case'));
    expect(schedule.ScheduleExpression).toBe('rate(480 minutes)');
    expect(schedule.FlexibleTimeWindow).toEqual({ Mode: 'FLEXIBLE', MaximumWindowInMinutes: 15 });
    expect(schedule).not.toHaveProperty('ScheduleExpressionTimezone');
    expect(schedule.Description).toBe('Triggers the step function every 480 minutes');
    expect(schedule.State).toBe('ENABLED');
  });

  it('deploys a member listed in disabledTables with the cron and state DISABLED', async () => {
    const schedule = scheduleOf(await synthTable('ehawk-mobilehub-1778525198-CaseEpisode', { syncGroupTimeUtc: '21:00', scheduleDisabled: true }));
    expect(schedule.ScheduleExpression).toBe('cron(5 5,13,21 * * ? *)');
    expect(schedule.State).toBe('DISABLED');
  });

  it('passes the group time to the time Lambda for a member only', async () => {
    const member = definitionOf(await synthTable('ehawk-mobilehub-1778525198-CaseEpisode', { syncGroupTimeUtc: '21:00' }));
    expect(member.States.GetNextIncrementalExportTime.Parameters.Payload.syncGroupTimeUtc).toBe('21:00');

    const nonMember = await synthTable('ehawk-mobilehub-1778525198-CaseEpisode');
    expect(Object.keys(nextWindowPayload(nonMember)).sort()).toEqual([
      'bucket', 'bucketOwner', 'bucketPrefix', 'executionId.$', 'exportFormat', 'exportViewType',
      'incrementalExportWindowSizeInMinutes', 'lastExportTime.$', 'tableArn',
    ]);

    // Apart from that one payload field, the member's definition is the non-member's.
    delete member.States.GetNextIncrementalExportTime.Parameters.Payload.syncGroupTimeUtc;
    expect(member).toEqual(definitionOf(nonMember));
  });
});
