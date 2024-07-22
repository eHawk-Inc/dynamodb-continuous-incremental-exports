import * as cdk from 'aws-cdk-lib';
import {
  aws_dynamodb as ddb,
  aws_iam as iam,
  aws_kms as kms,
  aws_sns as sns
} from "aws-cdk-lib";
import { Construct } from 'constructs';
import { Configuration, ContextConfiguration } from './configuration';
import { AwsServicePrincipals } from './constants/awsServicePrincipals';
interface DynamoDbContinuousIncrementalSharedResourceStackProps extends cdk.StackProps {
  configuration?: Configuration;
}

export class DynamoDbContinuousIncrementalSharedResourceStack extends cdk.Stack {

  private configuration: Configuration;
  public ddbExportNotificationTopic: sns.Topic;
  public kmsKeyUsedForSnsTopic: kms.Key;
  public schedulerRole: iam.Role;

  constructor(scope: Construct, id: string, props: DynamoDbContinuousIncrementalSharedResourceStackProps) {
    super(scope, id, props);

    this.init(props);
  }

  private async init(props: DynamoDbContinuousIncrementalSharedResourceStackProps) {
    this.configuration = props.configuration ?? new ContextConfiguration(this);

    this.kmsKeyUsedForSnsTopic = this.deployNotificationModule();

    this.schedulerRole = new iam.Role(this, 'step-function-trigger-role', {
      description: 'Roles used to triggers the step function scheduler',
      assumedBy: new iam.ServicePrincipal(AwsServicePrincipals.SCHEDULER, {
        conditions: {
          StringEquals: {
            'aws:SourceAccount': this.account
          }
        }
      }),
    });
  }

  private deployNotificationModule() : kms.Key {
    const snsKey = new kms.Key(this, 'ddb-export-notification-topic-key', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      pendingWindow: cdk.Duration.days(7),
      description: `Key for SSE for the notification topic used by incremental export for table`,
      enableKeyRotation: true
    });
    cdk.Tags.of(snsKey).add('Name', `${this.configuration.deploymentAlias}-ddb-export-notification-topic-key`);

    const topicName = `${this.configuration.deploymentAlias}-notification-topic`;
    this.ddbExportNotificationTopic = new sns.Topic(this, 'ddb-export-notification-topic', {
      displayName: topicName,
      topicName: topicName,
      enforceSSL: true,
      masterKey: snsKey
    });

    new cdk.aws_ssm.StringParameter(this, 'ddb-export-notification-topic-arn', {
      description: `ARN of the notification topic used by incremental export for all tables`,
      parameterName: `/dynamodb/export/notification/topic`,
      stringValue: this.ddbExportNotificationTopic.topicArn,
    });

    if(this.configuration.successNotificationSqsArn) {
      new sns.Subscription(this, 'ddb-export-notification-success-subsc-sqs', {
        topic: this.ddbExportNotificationTopic,
        endpoint: this.configuration.successNotificationSqsArn,
        protocol: sns.SubscriptionProtocol.SQS,
        filterPolicyWithMessageBody: { status: sns.FilterOrPolicy.filter(sns.SubscriptionFilter.stringFilter({ allowlist: ['SUCCESS'] })) }
      });
    }

    if(this.configuration.successNotificationEmail) {
      const successNotificationSub = new sns.Subscription(this, 'ddb-export-notification-success-subsc', {
        topic: this.ddbExportNotificationTopic,
        endpoint: this.configuration.successNotificationEmail,
        protocol: sns.SubscriptionProtocol.EMAIL,
        filterPolicyWithMessageBody: { status: sns.FilterOrPolicy.filter(sns.SubscriptionFilter.stringFilter({ allowlist: ['SUCCESS'] })) }
      });
    }

    if(this.configuration.failureNotificationEmail) {
      const failureNotificationSub = new sns.Subscription(this, 'ddb-export-notification-failure-subsc', {
        topic: this.ddbExportNotificationTopic,
        endpoint: this.configuration.failureNotificationEmail,
        protocol: sns.SubscriptionProtocol.EMAIL,
        filterPolicyWithMessageBody: { status: sns.FilterOrPolicy.filter(sns.SubscriptionFilter.stringFilter({ allowlist: ['FAILED'] })) }
      });
    }

    return snsKey;
  }
}
