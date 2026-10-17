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
