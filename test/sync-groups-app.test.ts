import { spawnSync } from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

const repoRoot = path.join(__dirname, '..');
const tables = [
  'ehawk-mobilehub-1778525198-CaseEpisode',
  'ehawk-mobilehub-1778525198-Appointments',
  'ehawk-mobilehub-1778525198-CasePrescription',
  'ehawk-mobilehub-1778525198-Case',
];

/** Synthesizes the real app entry point with the deploy's environment keys and returns its output directory. */
function synthApp(env: Record<string, string>, windowMinutes: number) {
  const outdir = fs.mkdtempSync(path.join(os.tmpdir(), 'sync-groups-app-'));
  const context = {
    stackName: 'prd-dynamodb-incremental',
    sourceDynamoDbTableName: tables.join(','),
    deploymentAlias: 'prd-us-dl-ingest',
    dataExportBucketOwnerAccountId: '111111111111',
    dataExportBucketName: 'example-trn-bucket',
    dataExportBucketPrefix: 'product/repath',
    incrementalExportWindowSizeInMinutes: String(windowMinutes),
  };
  const result = spawnSync(process.execPath, ['-r', 'ts-node/register', 'bin/dynamodb-continuous-incremental-exports.ts'], {
    cwd: repoRoot,
    encoding: 'utf8',
    env: {
      ...process.env,
      ENVIRONMENT_SHORT: '',
      EHAWK_REGION: '',
      ...env,
      CDK_OUTDIR: outdir,
      CDK_CONTEXT_JSON: JSON.stringify(context),
    },
  });
  return { outdir, status: result.status, stderr: result.stderr };
}

function nestedTemplate(outdir: string, table: string) {
  // Nested template files are named after the stack's logical id: the table name without dashes plus an 8-hex suffix.
  const pattern = new RegExp(`${table.replace(/-/g, '')}[0-9A-F]{8}\\.nested\\.template\\.json$`);
  const file = fs.readdirSync(outdir).find(name => pattern.test(name));
  if (file === undefined) throw new Error(`no nested template for ${table} in ${outdir}`);
  return JSON.parse(fs.readFileSync(path.join(outdir, file), 'utf8'));
}

function scheduleExpression(template: any) {
  return Object.values<any>(template.Resources).find(resource => resource.Type === 'AWS::Scheduler::Schedule').Properties.ScheduleExpression;
}

function payloadHasGroupTime(template: any) {
  const machine = Object.values<any>(template.Resources).find(resource => resource.Type === 'AWS::StepFunctions::StateMachine');
  return JSON.stringify(machine.Properties.DefinitionString).includes('syncGroupTimeUtc');
}

describe('app entry point', () => {
  it('gives prd-us members the group schedule and payload from the committed config, and leaves other tables alone', () => {
    const { outdir, status, stderr } = synthApp({ ENVIRONMENT_SHORT: 'prd', EHAWK_REGION: 'us' }, 1440);
    try {
      expect({ status, stderr }).toEqual({ status: 0, stderr: expect.any(String) });
      for (const member of tables.slice(0, 3)) {
        const template = nestedTemplate(outdir, member);
        expect(scheduleExpression(template)).toBe('cron(5 5,13,21 * * ? *)');
        expect(payloadHasGroupTime(template)).toBe(true);
      }
      const nonMember = nestedTemplate(outdir, 'ehawk-mobilehub-1778525198-Case');
      expect(scheduleExpression(nonMember)).toBe('rate(480 minutes)');
      expect(payloadHasGroupTime(nonMember)).toBe(false);
    } finally {
      fs.rmSync(outdir, { recursive: true, force: true });
    }
  }, 120_000);

  it('fails the synth when deployed members have a window other than 1440 minutes', () => {
    const { outdir, status, stderr } = synthApp({ ENVIRONMENT_SHORT: 'prd', EHAWK_REGION: 'us' }, 720);
    try {
      expect(status).not.toBe(0);
      expect(stderr).toMatch(/incrementalExportWindowSizeInMinutes 1440/);
    } finally {
      fs.rmSync(outdir, { recursive: true, force: true });
    }
  }, 120_000);
});
