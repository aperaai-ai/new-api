import {
  Stack,
  Fn,
  CfnParameter,
  CfnOutput,
  RemovalPolicy,
  aws_route53 as route53,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';
// Optional foundation: deploy once if the domain has no Route 53 hosted zone.
// Delegation at an external domain registrar still requires the domain owner's action.
export class DnsStack extends Stack {
  constructor(scope: Construct, id: string) {
    super(scope, id);
    const name = new CfnParameter(this, 'ZoneName');
    const zone = new route53.PublicHostedZone(this, 'Zone', {
      zoneName: name.valueAsString,
    });
    zone.applyRemovalPolicy(RemovalPolicy.RETAIN);
    new CfnOutput(this, 'HostedZoneId', { value: zone.hostedZoneId });
    new CfnOutput(this, 'Nameservers', {
      value: Fn.join(',', zone.hostedZoneNameServers!),
    });
  }
}
