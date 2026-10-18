import { App } from 'aws-cdk-lib';
import { DnsStack } from '../lib/dns-stack';
import { AperaStack } from '../lib/apera-stack';
const app = new App();
new AperaStack(app, 'AperaProduction', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION ?? 'us-east-1',
  },
  // Bootstrap with zero application tasks until images, schema and provider configuration exist.
  bootstrap: app.node.tryGetContext('bootstrap') === 'true',
});

if (app.node.tryGetContext('createDnsZone') === 'true')
  new DnsStack(app, 'AperaDns');
