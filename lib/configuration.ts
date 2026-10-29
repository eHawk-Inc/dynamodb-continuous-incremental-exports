import { Construct } from 'constructs';
import { IncrementalExportDefaults } from './constants/incrementalExportDefaults';
import { ExportViewType } from './constants/exportViewType';
import { ExportFormat } from './constants/exportFormat';

export interface Configuration {
  environmentName: string;
  sourceDynamoDbTableName: string;
  deploymentAlias: string;
  failureNotificationEmail: string;
  successNotificationEmail: string;
  successNotificationSqsArn?: string;
  failureNotificationSqsArn?: string;
  dataExportBucketOwnerAccountId?: string;
  dataExportBucketName: string;
  dataExportBucketPrefix: string;
  incrementalExportWindowSizeInMinutes: number;
  waitTimeToCheckExportStatusInSeconds: number;
  exportViewType: ExportViewType;
  exportFormat: ExportFormat;
  awsApiInvocationTimeoutInSeconds: number;
  /** Comma-separated context `disabledTables` entries; empty when unset. */
  disabledTables?: string[];
  /** Set per table by bin/ from `disabledTables`; the schedule is deployed DISABLED. */
  scheduleDisabled?: boolean;
}

export class ContextConfiguration implements Configuration{
  environmentName: string;
  sourceDynamoDbTableName: string;
  deploymentAlias: string;
  failureNotificationEmail: string;
  successNotificationEmail: string;
  successNotificationSqsArn?: string;
  failureNotificationSqsArn?: string;
  dataExportBucketOwnerAccountId?: string;
  dataExportBucketName: string;
  dataExportBucketPrefix: string;
  incrementalExportWindowSizeInMinutes: number;
  waitTimeToCheckExportStatusInSeconds: number;
  exportViewType: ExportViewType;
  exportFormat: ExportFormat;
  awsApiInvocationTimeoutInSeconds: number;
  disabledTables: string[];
  scheduleDisabled?: boolean;

  constructor(scope: Construct) {
    this.environmentName = process.env.ENVIRONMENT ?? "";
    this.sourceDynamoDbTableName = scope.node.tryGetContext('sourceDynamoDbTableName') as string;
    this.deploymentAlias = scope.node.tryGetContext('deploymentAlias') as string;
    this.failureNotificationEmail = scope.node.tryGetContext('failureNotificationEmail') as string;
    this.successNotificationEmail = scope.node.tryGetContext('successNotificationEmail') as string;
    this.successNotificationSqsArn = scope.node.tryGetContext('successNotificationSqsArn') as string;
    this.failureNotificationSqsArn = scope.node.tryGetContext('failureNotificationSqsArn') as string;
    this.dataExportBucketName = scope.node.tryGetContext('dataExportBucketName') as string;
    this.dataExportBucketOwnerAccountId = scope.node.tryGetContext('dataExportBucketOwnerAccountId') as string;
    this.dataExportBucketPrefix = scope.node.tryGetContext('dataExportBucketPrefix') as string;
    this.incrementalExportWindowSizeInMinutes = parseInt(scope.node.tryGetContext('incrementalExportWindowSizeInMinutes')) || 
      IncrementalExportDefaults.DEFAULT_INCREMENTAL_EXPORT_WINDOW_SIZE_IN_MINUTES;
    this.waitTimeToCheckExportStatusInSeconds = parseInt(scope.node.tryGetContext('waitTimeToCheckExportStatusInSeconds')) || 
      IncrementalExportDefaults.WAIT_TIME_TO_CHECK_EXPORT_STATUS_IN_SECONDS;
    this.exportFormat = scope.node.tryGetContext('exportFormat') as ExportFormat || 
      ExportFormat.DYNAMODB_JSON;
    this.exportViewType = scope.node.tryGetContext('exportViewType') as ExportViewType || 
      ExportViewType.NEW_AND_OLD_IMAGES;

    this.awsApiInvocationTimeoutInSeconds = parseInt(scope.node.tryGetContext('awsApiInvocationTimeoutInSeconds')) || 
      IncrementalExportDefaults.AWS_API_INVOCATION_TIMEOUT_IN_SECONDS;

    this.disabledTables = parseDisabledTables(scope.node.tryGetContext('disabledTables'));
  }
}

/** Parses the `disabledTables` context value (comma list, blanks ignored). */
export function parseDisabledTables(value: unknown): string[] {
  return String(value ?? '')
    .split(',')
    .map(entry => entry.trim())
    .filter(entry => entry.length > 0);
}

/** Last name segment, lowercased: `ehawk-mobilehub-123-RouteEntry` and `RouteEntry` both give `routeentry`. */
export function normalizeTableName(tableName: string): string {
  const parts = tableName.split(/[-_]/g);
  return parts[parts.length - 1].toLowerCase();
}

/** True when the table matches a disabledTables entry by full name or by normalized last segment. */
export function isTableDisabled(tableName: string, disabledTables: string[] = []): boolean {
  return disabledTables.some(entry =>
    entry === tableName || normalizeTableName(entry) === normalizeTableName(tableName));
}
