import {
  Stack,
  StackProps,
  Duration,
  RemovalPolicy,
  CfnParameter,
  CfnOutput,
  Tags,
  aws_ec2 as ec2,
  aws_ecs as ecs,
  aws_autoscaling as autoscaling,
  aws_ecr as ecr,
  aws_rds as rds,
  aws_elasticache as elasticache,
  aws_secretsmanager as secrets,
  aws_elasticloadbalancingv2 as elb,
  aws_certificatemanager as acm,
  aws_route53 as route53,
  aws_route53_targets as route53targets,
  aws_logs as logs,
  aws_cloudwatch as cw,
  aws_cloudwatch_actions as actions,
  aws_sns as sns,
  aws_iam as iam,
  aws_sqs as sqs,
  aws_s3 as s3,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';

export class AperaStack extends Stack {
  constructor(
    scope: Construct,
    id: string,
    props: StackProps & { bootstrap?: boolean } = {},
  ) {
    super(scope, id, props);
    const zoneId = new CfnParameter(this, 'HostedZoneId', {
      description: 'Authoritative Route 53 public hosted zone ID.',
    });
    const zoneName = new CfnParameter(this, 'HostedZoneName', {
      description: 'Parent zone, e.g. example.com.',
    });
    const imageTag = new CfnParameter(this, 'ImageTag', {
      default: 'bootstrap',
      description: 'Immutable Git SHA tag present in BOTH ECR repositories.',
    });
    const apiCount = new CfnParameter(this, 'ApiCount', {
      type: 'Number',
      default: 1,
      minValue: 1,
      maxValue: 10,
    });
    const apiDomain = new CfnParameter(this, 'ApiDomain', {
      description: 'Public HTTPS hostname, without scheme.',
    });
    const zone = route53.HostedZone.fromHostedZoneAttributes(
      this,
      'HostedZone',
      { hostedZoneId: zoneId.valueAsString, zoneName: zoneName.valueAsString },
    );
    const certificate = new acm.Certificate(this, 'Certificate', {
      domainName: apiDomain.valueAsString,
      validation: acm.CertificateValidation.fromDns(zone),
    });
    const vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways: 2,
      subnetConfiguration: [
        { name: 'public', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 },
        {
          name: 'application',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
          cidrMask: 24,
        },
        {
          name: 'data',
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
          cidrMask: 24,
        },
      ],
    });
    const apiSG = new ec2.SecurityGroup(this, 'ApiSecurityGroup', { vpc });
    const workerSG = new ec2.SecurityGroup(this, 'WorkerSecurityGroup', {
      vpc,
    });
    const database = new rds.DatabaseInstance(this, 'Database', {
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      engine: rds.DatabaseInstanceEngine.postgres({
        version: rds.PostgresEngineVersion.VER_16,
      }),
      instanceType: new ec2.InstanceType('t4g.medium'),
      multiAz: true,
      databaseName: 'apera',
      credentials: rds.Credentials.fromGeneratedSecret('apera_admin'),
      allocatedStorage: 20,
      maxAllocatedStorage: 200,
      storageType: rds.StorageType.GP3,
      storageEncrypted: true,
      publiclyAccessible: false,
      deletionProtection: true,
      backupRetention: Duration.days(14),
      copyTagsToSnapshot: true,
      removalPolicy: RemovalPolicy.RETAIN,
      parameters: { 'rds.force_ssl': '1' },
    });
    database.connections.allowDefaultPortFrom(apiSG);
    database.connections.allowDefaultPortFrom(workerSG);
    const redisSG = new ec2.SecurityGroup(this, 'RedisSecurityGroup', { vpc });
    redisSG.addIngressRule(apiSG, ec2.Port.tcp(6379));
    redisSG.addIngressRule(workerSG, ec2.Port.tcp(6379));
    const redisPassword = new secrets.Secret(this, 'RedisPassword', {
      generateSecretString: { passwordLength: 48, excludePunctuation: true },
      removalPolicy: RemovalPolicy.RETAIN,
    });
    const redisSubnet = new elasticache.CfnSubnetGroup(this, 'RedisSubnets', {
      description: 'Private data subnets',
      subnetIds: vpc.isolatedSubnets.map((s) => s.subnetId),
    });
    const redis = new elasticache.CfnReplicationGroup(this, 'Redis', {
      replicationGroupDescription: 'Apera HA Redis',
      engine: 'redis',
      engineVersion: '7.1',
      cacheNodeType: 'cache.t4g.small',
      numCacheClusters: 2,
      automaticFailoverEnabled: true,
      multiAzEnabled: true,
      atRestEncryptionEnabled: true,
      transitEncryptionEnabled: true,
      authToken: redisPassword.secretValue.unsafeUnwrap(),
      cacheSubnetGroupName: redisSubnet.ref,
      securityGroupIds: [redisSG.securityGroupId],
      snapshotRetentionLimit: 7,
    });
    redis.applyRemovalPolicy(RemovalPolicy.SNAPSHOT);
    const session = new secrets.Secret(this, 'SessionSecret', {
      generateSecretString: { passwordLength: 64, excludePunctuation: true },
      removalPolicy: RemovalPolicy.RETAIN,
    });
    const appPassword = new secrets.Secret(this, 'AppDatabasePassword', {
      generateSecretString: { passwordLength: 48, excludePunctuation: true },
      removalPolicy: RemovalPolicy.RETAIN,
    });
    const workerPassword = new secrets.Secret(this, 'WorkerDatabasePassword', {
      generateSecretString: { passwordLength: 48, excludePunctuation: true },
      removalPolicy: RemovalPolicy.RETAIN,
    });
    const crypto = new secrets.Secret(this, 'CryptoSecret', {
      generateSecretString: { passwordLength: 64, excludePunctuation: true },
      removalPolicy: RemovalPolicy.RETAIN,
    });
    const initialAdmin = new secrets.Secret(this, 'InitialAdminPassword', {
      generateSecretString: { passwordLength: 32, excludePunctuation: true },
      removalPolicy: RemovalPolicy.RETAIN,
    });
    const providers = new secrets.Secret(this, 'AsyncProviders', {
      description:
        'Provider registry JSON; disabled until configured. See docs/deployment/aws.md.',
      generateSecretString: {
        secretStringTemplate: '{"enabled":false,"accounts":{},"models":{}}',
        generateStringKey: '_nonce',
      },
      removalPolicy: RemovalPolicy.RETAIN,
    });
    // All supplier HTTP work is dispatched through this queue. Payloads stay
    // encrypted in private object storage; SQS messages contain execution IDs.
    const modelDLQ = new sqs.Queue(this, 'ModelExecutionDLQ', {
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
      retentionPeriod: Duration.days(14),
      removalPolicy: RemovalPolicy.RETAIN,
    });
    const modelQueue = new sqs.Queue(this, 'ModelExecutionQueue', {
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
      receiveMessageWaitTime: Duration.seconds(20),
      visibilityTimeout: Duration.seconds(120),
      retentionPeriod: Duration.days(4),
      deadLetterQueue: { queue: modelDLQ, maxReceiveCount: 8 },
      removalPolicy: RemovalPolicy.RETAIN,
    });
    const modelPayloads = new s3.Bucket(this, 'ModelPayloads', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      removalPolicy: RemovalPolicy.RETAIN,
      lifecycleRules: [
        {
          prefix: 'input/',
          expiration: Duration.days(1),
          abortIncompleteMultipartUploadAfter: Duration.days(1),
        },
        { prefix: 'output/', expiration: Duration.days(14) },
      ],
    });
    new CfnOutput(this, 'ModelQueueUrl', { value: modelQueue.queueUrl });
    new CfnOutput(this, 'ModelQueueArn', { value: modelQueue.queueArn });
    new CfnOutput(this, 'ModelDeadLetterQueueUrl', {
      value: modelDLQ.queueUrl,
    });
    new CfnOutput(this, 'ModelPayloadBucket', {
      value: modelPayloads.bucketName,
    });
    const cluster = new ecs.Cluster(this, 'Cluster', {
      vpc,
      containerInsightsV2: ecs.ContainerInsights.ENABLED,
    });
    const alarmTopic = new sns.Topic(this, 'AlarmTopic');
    const alarm = (name: string, metric: cw.IMetric, threshold: number) => {
      const a = new cw.Alarm(this, name, {
        metric,
        threshold,
        evaluationPeriods: 3,
        comparisonOperator: cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
        treatMissingData: cw.TreatMissingData.NOT_BREACHING,
      });
      a.addAlarmAction(new actions.SnsAction(alarmTopic));
      return a;
    };
    const services: Record<string, ecs.Ec2Service> = {};
    const images: Record<string, ecs.ContainerImage> = {};
    alarm(
      'ModelQueueAgeAlarm',
      modelQueue.metricApproximateAgeOfOldestMessage({
        period: Duration.minutes(1),
        statistic: 'Maximum',
      }),
      60,
    );
    alarm(
      'ModelDLQAlarm',
      modelDLQ.metricApproximateNumberOfMessagesVisible({
        period: Duration.minutes(1),
        statistic: 'Maximum',
      }),
      0,
    );
    for (const role of ['api', 'worker'] as const) {
      const isApi = role === 'api';
      const repo = new ecr.Repository(this, `${role}Repository`, {
        imageScanOnPush: true,
        imageTagMutability: ecr.TagMutability.IMMUTABLE,
        removalPolicy: RemovalPolicy.RETAIN,
      });
      const group = new autoscaling.AutoScalingGroup(this, `${role}Servers`, {
        vpc,
        vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
        instanceType: new ec2.InstanceType('t3.medium'),
        machineImage: ecs.EcsOptimizedImage.amazonLinux2023(),
        minCapacity: 0,
        maxCapacity: 10,
        requireImdsv2: true,
        newInstancesProtectedFromScaleIn: true,
        blockDevices: [
          {
            deviceName: '/dev/xvda',
            volume: autoscaling.BlockDeviceVolume.ebs(30, { encrypted: true }),
          },
        ],
      });
      group.role.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          'AmazonSSMManagedInstanceCore',
        ),
      );
      const capacity = new ecs.AsgCapacityProvider(this, `${role}Capacity`, {
        autoScalingGroup: group,
        enableManagedScaling: true,
        enableManagedTerminationProtection: true,
        targetCapacityPercent: 100,
        minimumScalingStepSize: 1,
        maximumScalingStepSize: 2,
      });
      cluster.addAsgCapacityProvider(capacity);
      images[role] = ecs.ContainerImage.fromEcrRepository(
        repo,
        imageTag.valueAsString,
      );
      const definition = new ecs.Ec2TaskDefinition(this, `${role}Task`, {
        networkMode: ecs.NetworkMode.AWS_VPC,
      });
      // Worker also publishes: its Tripo scheduler dispatches each submit/poll
      // HTTP operation through the same SQS transport, without direct bypass.
      modelQueue.grantSendMessages(definition.taskRole);
      modelPayloads.grantPut(definition.taskRole, 'input/*');
      modelPayloads.grantRead(definition.taskRole, 'output/*');
      if (!isApi) {
        modelQueue.grantConsumeMessages(definition.taskRole);
        modelPayloads.grantRead(definition.taskRole, 'input/*');
        modelPayloads.grantPut(definition.taskRole, 'output/*');
      }
      const logGroup = new logs.LogGroup(this, `${role}Logs`, {
        retention: logs.RetentionDays.ONE_MONTH,
        removalPolicy: RemovalPolicy.RETAIN,
      });
      new logs.MetricFilter(this, `${role}ModelErrors`, {
        logGroup,
        filterPattern: logs.FilterPattern.anyTerm(
          'model execution outcome unknown',
          'model execution failed',
          'model queue receive failed',
          'model queue publish failed',
          'model billing hold requires reconciliation',
          'model hold release requires reconciliation',
        ),
        metricNamespace: 'Apera/Model',
        metricName: `${role}ExecutionErrors`,
        metricValue: '1',
      });
      alarm(
        `${role}ModelErrorsAlarm`,
        new cw.Metric({
          namespace: 'Apera/Model',
          metricName: `${role}ExecutionErrors`,
          statistic: 'Sum',
          period: Duration.minutes(1),
        }),
        0,
      );
      if (!isApi) {
        new logs.MetricFilter(this, 'WorkerErrorsFilter', {
          logGroup,
          filterPattern: logs.FilterPattern.anyTerm(
            'async account processing failed',
            'async worker step failed',
            'async provider rejected request',
          ),
          metricNamespace: 'Apera/Async',
          metricName: 'WorkerErrors',
          metricValue: '1',
        });
        alarm(
          'WorkerErrorsAlarm',
          new cw.Metric({
            namespace: 'Apera/Async',
            metricName: 'WorkerErrors',
            statistic: 'Sum',
            period: Duration.minutes(1),
          }),
          0,
        );

        for (const [field, metricName, threshold] of [
          ['queued', 'QueuedTasks', 100],
          ['reconciling', 'ReconcilingTasks', 0],
          ['oldest_queued_seconds', 'OldestQueuedSeconds', 300],
        ] as const) {
          new logs.MetricFilter(this, `${metricName}Filter`, {
            logGroup,
            filterPattern: logs.FilterPattern.stringValue(
              '$.event',
              '=',
              'async_queue_metrics',
            ),
            metricNamespace: 'Apera/Async',
            metricName,
            metricValue: `$.${field}`,
          });
          alarm(
            `${metricName}Alarm`,
            new cw.Metric({
              namespace: 'Apera/Async',
              metricName,
              statistic: 'Maximum',
              period: Duration.minutes(1),
            }),
            threshold,
          );
        }
      }
      const container = definition.addContainer(role, {
        image: images[role],
        memoryLimitMiB: 2048,
        cpu: isApi ? 1024 : 512,
        stopTimeout: Duration.seconds(120),
        environment: {
          VERSION: imageTag.valueAsString,
          TRUSTED_PROXY_CIDRS: vpc.vpcCidrBlock,
          APERA_ROLE: role,
          MODEL_QUEUE_ENABLED: 'true',
          MODEL_QUEUE_URL: modelQueue.queueUrl,
          MODEL_PAYLOAD_BUCKET: modelPayloads.bucketName,
          MODEL_WORKER_CONCURRENCY: '4',
          AWS_REGION: this.region,
          NODE_TYPE: 'slave',
          PORT: '3000',
          GIN_MODE: 'release',
          SQL_HOST: database.dbInstanceEndpointAddress,
          SQL_PORT: '5432',
          SQL_DATABASE: 'apera',
          SQL_USER: isApi ? 'apera_app' : 'apera_worker',
          SQL_SSL_ROOT_CERT: '/etc/ssl/certs/rds-ca.pem',
          SQL_MAX_OPEN_CONNS: '15',
          SQL_MAX_IDLE_CONNS: '5',
          SQL_MAX_LIFETIME: '300',
          REDIS_CONN_STRING: `rediss://${redis.attrPrimaryEndPointAddress}:6379/0`,
          BATCH_UPDATE_ENABLED: 'false',
          SESSION_COOKIE_SECURE: 'true',
        },
        secrets: {
          SQL_PASSWORD: ecs.Secret.fromSecretsManager(
            isApi ? appPassword : workerPassword,
          ),
          SESSION_SECRET: ecs.Secret.fromSecretsManager(session),
          CRYPTO_SECRET: ecs.Secret.fromSecretsManager(crypto),
          REDIS_AUTH_TOKEN: ecs.Secret.fromSecretsManager(redisPassword),
          ASYNC_PROVIDERS_JSON: ecs.Secret.fromSecretsManager(providers),
        },
        logging: ecs.LogDrivers.awsLogs({ logGroup, streamPrefix: role }),
        healthCheck: {
          command: ['CMD', '/apera-healthcheck'],
          interval: Duration.seconds(30),
          timeout: Duration.seconds(5),
          retries: 3,
          startPeriod: Duration.seconds(60),
        },
      });
      if (isApi) container.addPortMappings({ containerPort: 3000 });
      const service = new ecs.Ec2Service(this, `${role}Service`, {
        cluster,
        taskDefinition: definition,
        desiredCount: props.bootstrap ? 0 : isApi ? apiCount.valueAsNumber : 2,
        capacityProviderStrategies: [
          { capacityProvider: capacity.capacityProviderName, weight: 1 },
        ],
        placementConstraints: [ecs.PlacementConstraint.distinctInstances()],
        placementStrategies: [
          ecs.PlacementStrategy.spreadAcross(
            ecs.BuiltInAttributes.AVAILABILITY_ZONE,
          ),
        ],
        securityGroups: [isApi ? apiSG : workerSG],
        vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
        circuitBreaker: { rollback: true },
        minHealthyPercent: 100,
        maxHealthyPercent: 200,
        healthCheckGracePeriod: isApi ? Duration.seconds(90) : undefined,
      });
      // Prepare infrastructure/secrets/migration permissions while keeping the
      // currently running application revisions until schema migration succeeds.
      const pinnedDefinition = this.node.tryGetContext(
        `prepare${isApi ? 'Api' : 'Worker'}TaskDefinition`,
      );
      if (pinnedDefinition) {
        if (
          typeof pinnedDefinition !== 'string' ||
          !/^arn:[^:]+:ecs:[^:]+:[0-9]+:task-definition\//.test(
            pinnedDefinition,
          )
        )
          throw new Error('Invalid prepare task definition ARN');
        const cfnService = service.node.defaultChild as ecs.CfnService;
        cfnService.taskDefinition = pinnedDefinition;
        const count = Number(
          this.node.tryGetContext(`prepare${isApi ? 'Api' : 'Worker'}Count`),
        );
        if (!Number.isInteger(count) || count < 0 || count > 100)
          throw new Error('Invalid prepare service count');
        cfnService.desiredCount = count;
      }
      // API scales on CPU initially. Worker remains bounded at two until an account-aware queue policy is configured.
      if (!props.bootstrap && isApi)
        service
          .autoScaleTaskCount({
            minCapacity: apiCount.valueAsNumber,
            maxCapacity: 10,
          })
          .scaleOnCpuUtilization('CpuScaling', {
            targetUtilizationPercent: 60,
            scaleInCooldown: Duration.minutes(5),
          });
      services[role] = service;
      new CfnOutput(this, `${role}CapacityProvider`, {
        value: capacity.capacityProviderName,
      });
      alarm(
        `${role}CpuAlarm`,
        service.metricCpuUtilization({ period: Duration.minutes(1) }),
        85,
      );
      new CfnOutput(this, `${role}RepositoryUri`, {
        value: repo.repositoryUri,
      });
      new CfnOutput(this, `${role}ServiceName`, { value: service.serviceName });
    }
    // Queue consumers must be healthy before routing traffic to a new API revision.
    services.api.node.addDependency(services.worker);
    for (const association of cluster.node
      .findAll()
      .filter((c) => c instanceof ecs.CfnClusterCapacityProviderAssociations)) {
      for (const service of Object.values(services))
        service.node.addDependency(association);
    }
    // Application DB user credentials are distinct from the RDS master credentials.
    // The migration task provisions this role before any services start.
    new CfnOutput(this, 'AppCredentialsSecretArn', {
      value: appPassword.secretArn,
    });
    const alb = new elb.ApplicationLoadBalancer(this, 'LoadBalancer', {
      vpc,
      internetFacing: true,
    });
    alb.addListener('Http', {
      port: 80,
      defaultAction: elb.ListenerAction.redirect({
        protocol: 'HTTPS',
        port: '443',
        permanent: true,
      }),
    });
    const https = alb.addListener('Https', {
      port: 443,
      certificates: [certificate],
    });
    const targets = https.addTargets('ApiTargets', {
      port: 3000,
      protocol: elb.ApplicationProtocol.HTTP,
      targets: [services.api],
      deregistrationDelay: Duration.seconds(60),
      healthCheck: { path: '/health/ready', healthyHttpCodes: '200' },
    });
    alb.setAttribute('idle_timeout.timeout_seconds', '300');
    new route53.ARecord(this, 'ApiDnsRecord', {
      zone,
      recordName: apiDomain.valueAsString,
      target: route53.RecordTarget.fromAlias(
        new route53targets.LoadBalancerTarget(alb),
      ),
    });
    const migration = new ecs.Ec2TaskDefinition(this, 'MigrationTask', {
      networkMode: ecs.NetworkMode.AWS_VPC,
    });
    // Run with API image, but never as a continuously restarting service.
    migration.addContainer('migrate', {
      image: images.api,
      command: ['/apera-migrate'],
      memoryLimitMiB: 1024,
      cpu: 512,
      environment: {
        APERA_PUBLIC_URL: `https://${apiDomain.valueAsString}`,
        SQL_HOST: database.dbInstanceEndpointAddress,
        SQL_DATABASE: 'apera',
        SQL_USER: 'apera_admin',
        SQL_SSL_ROOT_CERT: '/etc/ssl/certs/rds-ca.pem',
      },
      secrets: {
        SQL_PASSWORD: ecs.Secret.fromSecretsManager(
          database.secret!,
          'password',
        ),
        APP_DB_PASSWORD: ecs.Secret.fromSecretsManager(appPassword),
        WORKER_DB_PASSWORD: ecs.Secret.fromSecretsManager(workerPassword),
        APERA_INITIAL_ADMIN_PASSWORD:
          ecs.Secret.fromSecretsManager(initialAdmin),
      },
      logging: ecs.LogDrivers.awsLogs({
        streamPrefix: 'migration',
        logRetention: logs.RetentionDays.ONE_MONTH,
      }),
    });
    alarm('DatabaseCpuAlarm', database.metricCPUUtilization(), 80);
    alarm('ApiUnhealthyAlarm', targets.metrics.unhealthyHostCount(), 0);
    new cw.Dashboard(this, 'Dashboard', {
      widgets: [
        [
          new cw.GraphWidget({
            title: 'API and Worker CPU',
            left: [
              services.api.metricCpuUtilization(),
              services.worker.metricCpuUtilization(),
            ],
          }),
        ],
        [
          new cw.GraphWidget({
            title: 'SQS backlog / dead letters',
            left: [
              modelQueue.metricApproximateNumberOfMessagesVisible(),
              modelQueue.metricApproximateNumberOfMessagesNotVisible(),
              modelDLQ.metricApproximateNumberOfMessagesVisible(),
            ],
          }),
        ],
        [
          new cw.GraphWidget({
            title: 'Async queue / reconciliation',
            left: ['QueuedTasks', 'ReconcilingTasks'].map(
              (metricName) =>
                new cw.Metric({
                  namespace: 'Apera/Async',
                  metricName,
                  statistic: 'Maximum',
                }),
            ),
          }),
        ],
        [
          new cw.GraphWidget({
            title: 'Database connections',
            left: [database.metricDatabaseConnections()],
          }),
        ],
        [
          new cw.GraphWidget({
            title: 'API latency',
            left: [targets.metrics.targetResponseTime()],
          }),
        ],
      ],
    });
    new CfnOutput(this, 'AlbHostname', { value: alb.loadBalancerDnsName });
    new CfnOutput(this, 'ClusterName', { value: cluster.clusterName });
    new CfnOutput(this, 'ApiSecurityGroupId', { value: apiSG.securityGroupId });
    new CfnOutput(this, 'ApplicationSubnetIds', {
      value: vpc.privateSubnets.map((s) => s.subnetId).join(','),
    });
    new CfnOutput(this, 'MigrationTaskArn', {
      value: migration.taskDefinitionArn,
    });
    new CfnOutput(this, 'ProviderSecretArn', { value: providers.secretArn });
    new CfnOutput(this, 'InitialAdminSecretArn', {
      value: initialAdmin.secretArn,
    });
    new CfnOutput(this, 'DatabaseAdminSecretArn', {
      value: database.secret!.secretArn,
    });
    new CfnOutput(this, 'DatabaseHostname', {
      value: database.dbInstanceEndpointAddress,
    });
    new CfnOutput(this, 'AlarmTopicArn', { value: alarmTopic.topicArn });
    Tags.of(this).add('Application', 'apera');
  }
}
