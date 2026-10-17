#!/usr/bin/env python3
"""Run the migration image before rolling application services. Requires AWS CLI.

Never prints secret values. The existing task definition supplies secret ARNs.
An updated image is registered as a new revision of the migration family only.
"""
import argparse
import json
import subprocess
import time


def aws(*args):
    return json.loads(subprocess.check_output(['aws', *args, '--output', 'json']))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stack', default='AperaProduction')
    parser.add_argument('--image-tag', required=True)
    args = parser.parse_args()
    stack = aws('cloudformation', 'describe-stacks', '--stack-name', args.stack)['Stacks'][0]
    outputs = {v['OutputKey']: v['OutputValue'] for v in stack['Outputs']}
    definition = aws('ecs', 'describe-task-definition', '--task-definition', outputs['MigrationTaskArn'])['taskDefinition']
    allowed = {'family', 'taskRoleArn', 'executionRoleArn', 'networkMode', 'containerDefinitions',
               'volumes', 'placementConstraints', 'requiresCompatibilities', 'cpu', 'memory',
               'runtimePlatform', 'ephemeralStorage', 'proxyConfiguration', 'ipcMode', 'pidMode'}
    request = {k: v for k, v in definition.items() if k in allowed}
    request['containerDefinitions'][0]['image'] = outputs['apiRepositoryUri'] + ':' + args.image_tag
    task_def = aws('ecs', 'register-task-definition', '--cli-input-json', json.dumps(request))['taskDefinition']['taskDefinitionArn']
    network = {'awsvpcConfiguration': {'subnets': outputs['ApplicationSubnetIds'].split(','),
               'securityGroups': [outputs['ApiSecurityGroupId']], 'assignPublicIp': 'DISABLED'}}
    result = aws('ecs', 'run-task', '--cluster', outputs['ClusterName'], '--task-definition', task_def,
                 '--capacity-provider-strategy', json.dumps([{'capacityProvider': outputs['apiCapacityProvider'], 'weight': 1}]),
                 '--network-configuration', json.dumps(network))
    if result.get('failures') or not result.get('tasks'):
        raise RuntimeError('Migration could not be scheduled; inspect ECS events')
    arn = result['tasks'][0]['taskArn']
    deadline = time.monotonic() + 1800
    while time.monotonic() < deadline:
        task = aws('ecs', 'describe-tasks', '--cluster', outputs['ClusterName'], '--tasks', arn)['tasks'][0]
        if task['lastStatus'] == 'STOPPED':
            containers = task.get('containers', [])
            if not containers or any(c.get('exitCode') != 0 for c in containers):
                raise RuntimeError('Migration failed; inspect the migration CloudWatch log stream')
            print('Migration completed:', arn)
            return
        time.sleep(10)
    aws('ecs', 'stop-task', '--cluster', outputs['ClusterName'], '--task', arn, '--reason', 'Migration deployment deadline exceeded')
    raise RuntimeError('Migration timed out; deployment stopped')


if __name__ == '__main__':
    main()
