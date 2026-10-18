import { test } from 'node:test';
import { App } from 'aws-cdk-lib';
import { Template, Match } from 'aws-cdk-lib/assertions';
import { AperaStack } from '../lib/apera-stack';

test('production uses isolated data, HA storage, separate compute pools and TLS', () => {
  const template = Template.fromStack(new AperaStack(new App(), 'Test'));
  template.resourceCountIs('AWS::EC2::NatGateway', 2);
  template.resourceCountIs('AWS::AutoScaling::AutoScalingGroup', 2);
  template.resourceCountIs('AWS::ECS::CapacityProvider', 2);
  template.resourceCountIs('AWS::ECS::Service', 2);
  template.hasResourceProperties('AWS::RDS::DBInstance', {
    MultiAZ: true,
    PubliclyAccessible: false,
    StorageEncrypted: true,
    DeletionProtection: true,
    BackupRetentionPeriod: 14,
  });
  template.hasResourceProperties('AWS::ElastiCache::ReplicationGroup', {
    MultiAZEnabled: true,
    AutomaticFailoverEnabled: true,
    NumCacheClusters: 2,
    TransitEncryptionEnabled: true,
    AtRestEncryptionEnabled: true,
  });
  template.hasResourceProperties('AWS::ElasticLoadBalancingV2::Listener', {
    Port: 443,
    Protocol: 'HTTPS',
  });
  template.hasResourceProperties('AWS::ECS::Service', {
    DesiredCount: 2,
    PlacementConstraints: [{ Type: 'distinctInstance' }],
  });
  template.hasResourceProperties('AWS::ECS::TaskDefinition', {
    ContainerDefinitions: Match.arrayWith([
      Match.objectLike({
        Name: 'migrate',
        Command: ['/apera-migrate'],
        Secrets: Match.arrayWith([
          Match.objectLike({ Name: 'APERA_INITIAL_ADMIN_PASSWORD' }),
        ]),
      }),
    ]),
  });
});
test('bootstrap never launches services before schema and images exist', () => {
  const template = Template.fromStack(
    new AperaStack(new App(), 'Bootstrap', { bootstrap: true }),
  );
  const services = template.findResources('AWS::ECS::Service');
  for (const service of Object.values(services)) {
    if (service.Properties.DesiredCount !== 0)
      throw new Error('bootstrap launched application');
  }
});

test('model executions use a new encrypted SQS queue, DLQ and private payload bucket', () => {
  const template = Template.fromStack(new AperaStack(new App(), 'QueueTest'));
  template.resourceCountIs('AWS::SQS::Queue', 2);
  template.hasResourceProperties('AWS::SQS::Queue', {
    SqsManagedSseEnabled: true,
    VisibilityTimeout: 120,
    ReceiveMessageWaitTimeSeconds: 20,
    RedrivePolicy: {
      deadLetterTargetArn: Match.anyValue(),
      maxReceiveCount: 8,
    },
  });
  template.hasResourceProperties('AWS::S3::Bucket', {
    PublicAccessBlockConfiguration: {
      BlockPublicAcls: true,
      BlockPublicPolicy: true,
      IgnorePublicAcls: true,
      RestrictPublicBuckets: true,
    },
    BucketEncryption: Match.anyValue(),
    LifecycleConfiguration: {
      Rules: Match.arrayWith([Match.objectLike({ ExpirationInDays: 14 })]),
    },
  });
  for (const role of ['api', 'worker']) {
    template.hasResourceProperties('AWS::ECS::TaskDefinition', {
      ContainerDefinitions: Match.arrayWith([
        Match.objectLike({
          Name: role,
          Environment: Match.arrayWith([
            { Name: 'MODEL_QUEUE_ENABLED', Value: 'true' },
            { Name: 'MODEL_QUEUE_URL', Value: Match.anyValue() },
            { Name: 'MODEL_PAYLOAD_BUCKET', Value: Match.anyValue() },
          ]),
        }),
      ]),
    });
  }
});

test('prepare deployment preserves running revisions before migrating', () => {
  const app = new App({
    context: {
      prepareApiTaskDefinition:
        'arn:aws:ecs:us-east-1:123456789012:task-definition/current-api:7',
      prepareWorkerTaskDefinition:
        'arn:aws:ecs:us-east-1:123456789012:task-definition/current-worker:5',
      prepareApiCount: 3,
      prepareWorkerCount: 2,
    },
  });
  const template = Template.fromStack(new AperaStack(app, 'Prepared'));
  template.hasResourceProperties('AWS::ECS::Service', {
    TaskDefinition:
      'arn:aws:ecs:us-east-1:123456789012:task-definition/current-api:7',
    DesiredCount: 3,
  });
  template.hasResourceProperties('AWS::ECS::Service', {
    TaskDefinition:
      'arn:aws:ecs:us-east-1:123456789012:task-definition/current-worker:5',
    DesiredCount: 2,
  });
  template.hasResourceProperties('AWS::ECS::TaskDefinition', {
    ContainerDefinitions: Match.arrayWith([
      Match.objectLike({
        Name: 'migrate',
        Secrets: Match.arrayWith([
          Match.objectLike({ Name: 'WORKER_DB_PASSWORD' }),
        ]),
      }),
    ]),
  });
});
