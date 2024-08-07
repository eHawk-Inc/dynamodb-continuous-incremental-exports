#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { AwsSolutionsChecks } from 'cdk-nag'
import { Aspects } from 'aws-cdk-lib';
import { DynamoDbContinuousIncrementalExportsStack } from '../lib/dynamodb-continuous-incremental-exports-stack';
import { DynamoDbContinuousIncrementalSharedResourceStack } from '../lib/dynamodb-continuous-incremental-shared-resources-stack';
import { NagSuppressions } from 'cdk-nag';
import { ContextConfiguration } from '../lib/configuration';

const app = new cdk.App();

Aspects.of(app).add(new AwsSolutionsChecks({ verbose: true }));

const configuration = new ContextConfiguration(app);
const stackName = app.node.tryGetContext('stackName') as string;

cdk.Tags.of(app).add("Environment", configuration.environmentName);
cdk.Tags.of(app).add("repository_url", "https://github.com/eHawk-Inc/dynamodb-continuous-incremental-exports");
cdk.Tags.of(app).add("cost:category", "datalake");
cdk.Tags.of(app).add("cost:category_sub", "etl");
cdk.Tags.of(app).add("cost:category_area", "dynamodb");
cdk.Tags.of(app).add("cost:team", "eHawk");

const incrementalExportSharedResourcesStack = new DynamoDbContinuousIncrementalSharedResourceStack(app, `${stackName}-export`, {
    configuration: configuration,
});

configuration.sourceDynamoDbTableName
    .split(',')
    .filter(tableName => tableName.trim().length > 0)
    .forEach(tableName => {
        const tableNameParts = tableName.split("-");
        const shortTableName = (tableName.length > 10 ? tableNameParts[tableNameParts.length - 1] : tableName).toLowerCase();
        
        const incrementalExportStack = new DynamoDbContinuousIncrementalExportsStack(incrementalExportSharedResourcesStack, tableName, {
            kmsKeyUsedForSnsTopic: incrementalExportSharedResourcesStack.kmsKeyUsedForSnsTopic,
            ddbExportNotificationTopic: incrementalExportSharedResourcesStack.ddbExportNotificationTopic,
            schedulerRole: incrementalExportSharedResourcesStack.schedulerRole,
            configuration: {
                ...configuration,
                deploymentAlias: `${configuration.deploymentAlias}-${shortTableName}`,
                sourceDynamoDbTableName: tableName,
                dataExportBucketPrefix: `${configuration.dataExportBucketPrefix}/${tableName}`,
                dataExportBucketOwnerAccountId: configuration.dataExportBucketOwnerAccountId,
            }
        });
        cdk.Tags.of(incrementalExportStack).add("cost:category_area", tableName);

        NagSuppressions.addStackSuppressions(incrementalExportStack, [
            {
                id: 'AwsSolutions-IAM5',
                reason: 'Tasks added to the step function add the appropriate policies to the IAM role used by the Step Function',
            }
        ]);
    });

