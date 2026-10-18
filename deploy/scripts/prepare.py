#!/usr/bin/env python3
"""Prepare new resources without moving ECS services to unmigrated code.

CDK registers future task definitions and grants the migration role access to new
secrets; services retain their current revisions until the final deploy step.
"""
import argparse
import os
import subprocess
from migrate import aws


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stack', default='AperaProduction')
    parser.add_argument('--image-tag', required=True)
    args = parser.parse_args()
    stack = aws('cloudformation', 'describe-stacks', '--stack-name', args.stack)['Stacks'][0]
    outputs = {v['OutputKey']: v['OutputValue'] for v in stack['Outputs']}
    context = []
    for role in ('api', 'worker'):
        services = aws('ecs', 'describe-services', '--cluster', outputs['ClusterName'],
                       '--services', outputs[role + 'ServiceName'])
        if services.get('failures') or len(services.get('services', [])) != 1:
            raise RuntimeError('Cannot pin current service revision; deployment stopped')
        service = services['services'][0]
        context.extend(['-c', 'prepare' + role.title() + 'TaskDefinition=' + service['taskDefinition'],
                        '-c', 'prepare' + role.title() + 'Count=' + str(service['desiredCount'])])
    command = ['npx', 'cdk', 'deploy', args.stack, '--require-approval', 'never',
               '--parameters', 'ImageTag=' + args.image_tag]
    for name, env in [('HostedZoneId', 'HOSTED_ZONE_ID'), ('HostedZoneName', 'HOSTED_ZONE_NAME'),
                      ('ApiDomain', 'API_DOMAIN'), ('ApiCount', 'API_COUNT')]:
        value = os.environ.get(env)
        if not value:
            raise RuntimeError('Missing deployment setting: ' + env)
        command.extend(['--parameters', name + '=' + value])
    subprocess.run(command + context, cwd='infra', check=True)


if __name__ == '__main__':
    main()
