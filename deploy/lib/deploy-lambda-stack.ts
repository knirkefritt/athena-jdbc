import * as path from 'path';
import * as cdk from '@aws-cdk/core';
import * as lambda from '@aws-cdk/aws-lambda';
import * as s3 from '@aws-cdk/aws-s3';
import * as iam from '@aws-cdk/aws-iam';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';

interface IJdbcCatalog {
  name: string,
  account: string,
  connectionString: string,
  assumeRoleArn: string,
  metaConnectionString: string,
  metaAssumeRoleArn: string,
  metaSecret: secretsmanager.ISecret,
}

export class DeployLambdaStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const connectorExecutionRole = new iam.Role(this, 'athena-jdbc-poc-conn-role', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      roleName: 'athena-lambda-execution-role-poc',
    });

    const roleToPassPrincipalTags = new iam.Role(this, 'athena-jdbc-poc', {
      assumedBy: connectorExecutionRole,
      roleName: 'athena-poc-pass-tags',
      description: 'A role used by the athena jdbc connector to federate auth to the datasets',
    });    

    const metadataQueryRole = new iam.Role(this, 'athena-jdbc-meta-poc', {
      assumedBy: connectorExecutionRole,
      roleName: 'athena-poc-meta',
      description: 'A role used by the athena jdbc connector to fetch metadata from the underlying data source',
    });    

    const metaSecret = secretsmanager.Secret.fromSecretCompleteArn(
      this,
      'get-secret-for-metadata-query',
      cdk.Fn.importValue('secret-with-db-cred-arn')
    );
    const host = cdk.Fn.importValue('testdb-aurora-reader-db-endpoint');
    const port = 5432;

    const catalog = {
      name: 'testdb',
      account: '123456789012',
      connectionString: `postgres://jdbc:postgresql://${host}:${port}/testdb?user=testdb-user&password=%s`,
      assumeRoleArn: roleToPassPrincipalTags.roleArn,
      metaConnectionString: `postgres://jdbc:postgresql://${host}:${port}/testdb?\$\{${metaSecret.secretArn}\}`,
      metaAssumeRoleArn: metadataQueryRole.roleArn,
      metaSecret,
    };

    const spillBucket = this.createSpillBucket();

    this.configureLambdaExecutionRole(connectorExecutionRole, roleToPassPrincipalTags, spillBucket);
    this.configureABACRole(roleToPassPrincipalTags, connectorExecutionRole);
    this.configurePrincipalAuth(roleToPassPrincipalTags, catalog);
    this.configureMetadataRole(metadataQueryRole, catalog);

    const athenaConnector = this.deployLambda(connectorExecutionRole, spillBucket, catalog);
    this.configureDbClusterToAllowConnectionsFromLambda(athenaConnector);
  }

  private createSpillBucket() {
    return new s3.Bucket(this, 'athena-spill-bucket', {
      enforceSSL: true,
      bucketName: 'testdb-athena-spill-bucket-poc',
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      autoDeleteObjects: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          enabled: true,
          expiration: cdk.Duration.days(1),
        }
      ]
    });
  }

  private configurePrincipalAuth(roleRepresentingQueryExecutor: iam.Role, catalog: IJdbcCatalog) {
    const dbClusterResourceId = 'cluster-RESOURCEID';
    const dbUserRepresentingRole = 'testdb-user';

    roleRepresentingQueryExecutor.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['rds-db:connect'],
      resources: [
        `arn:aws:rds-db:eu-west-1:${catalog.account}:dbuser:${dbClusterResourceId}/${dbUserRepresentingRole}`
      ],
      conditions: {
        "ForAnyValue:StringEquals" : {
          "aws:PrincipalTag/testdb-user": "true",
          "aws:PrincipalTag/testdb-admin": "true"
        }
      },
    }));
  }

  private configureMetadataRole(metadataQueryRole: iam.Role, catalog: IJdbcCatalog) {
    catalog.metaSecret.grantRead(metadataQueryRole);
    return metadataQueryRole;
  }

  /**
   * This is the role which will be assumed by the lambda before executing a query against the underlying data  
   */
  private configureABACRole(roleToPassPrincipalTags: iam.Role, connectorExecutionRole: iam.Role) {
    // https://docs.aws.amazon.com/IAM/latest/UserGuide/id_session-tags.html#id_session-tags_permissions-required
    roleToPassPrincipalTags.assumeRolePolicy!.addStatements(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['sts:TagSession'],
      principals: [
        new iam.ArnPrincipal(connectorExecutionRole.roleArn),
      ],
    }));

    return roleToPassPrincipalTags;
  }

  private configureLambdaExecutionRole(connectorExecutionRole: iam.Role, roleToPassPrincipalTags: iam.Role, spillBucket: s3.Bucket) {
    connectorExecutionRole.addManagedPolicy(iam.ManagedPolicy.fromManagedPolicyArn(this, 'import-lambda-basic-exec', 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'));
    connectorExecutionRole.addManagedPolicy(iam.ManagedPolicy.fromManagedPolicyArn(this, 'import-lambda-vpc-access', 'arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole'));

    // Athena connector specifics
    connectorExecutionRole.addToPolicy(new iam.PolicyStatement({
      sid: 'AllowLambdaAccessToSpillBucket',
      effect: iam.Effect.ALLOW,
      actions: [
        's3:GetBucketLocation',
        's3:GetObject',
        's3:ListBucket',
        's3:PutObject',
        's3:ListMultipartUploadParts',
        's3:AbortMultipartUpload',
      ],
      resources: [
        spillBucket.bucketArn
      ]
    }));

    // Athena connector specifics
    connectorExecutionRole.addToPolicy(new iam.PolicyStatement({
      sid: 'SpillBucketVerifierNeedsThis',
      effect: iam.Effect.ALLOW,
      actions: [
        's3:ListAllMyBuckets',
        'athena:GetQueryExecution',
      ],
      resources: ['*']
    }));

    // Our trick to use an ABAC access model to enforce security based on the caller, not the lambda execution role
    connectorExecutionRole.addToPolicy(new iam.PolicyStatement({
      sid: 'ConnectorNeedsThisToPassExecutorsPrincipalTags',
      effect: iam.Effect.ALLOW,
      actions: [
        'sts:TagSession',
      ],
      resources: [`arn:aws:iam::${this.account}:role/${roleToPassPrincipalTags.roleName}`]
    }));
    return connectorExecutionRole;
  }

  private deployLambda(
    connectorExecutionRole: iam.Role, 
    spillBucket: s3.Bucket, 
    catalog: IJdbcCatalog) {

    const connector = new lambda.Function(this, 'athena-jdbc-connector', {
      role: connectorExecutionRole,
      code: lambda.Code.fromAsset(path.join(__dirname, '../..'), {
        bundling: {
          image: lambda.Runtime.JAVA_11.bundlingImage,
          command: [
            '/bin/sh', '-c',
            'mvn clean install && cp /asset-input/target/athena-jdbc-0.0.1.jar /asset-output/'
          ]
        }
      }),
      runtime: lambda.Runtime.JAVA_11,
      handler: 'com.amazonaws.connectors.athena.jdbc.MultiplexingJdbcCompositeHandler',
      environment: {
        'disable_spill_encryption': 'false',
        'spill_bucket': spillBucket.bucketName,
        'spill_prefix': '',
        'testdb_connection_string': catalog.connectionString,
        'testdb_assume_role_arn': catalog.assumeRoleArn,
        'testdb_meta_connection_string': catalog.metaConnectionString || catalog.connectionString,
        'testdb_meta_assume_role_arn': catalog.metaAssumeRoleArn || catalog.assumeRoleArn,
        'default': `postgres://jdbc:postgresql://a_teapot:5432/testdb?username=bob`,
      },
      vpc: ec2.Vpc.fromLookup(this, 'managed-vpc', { tags: { 'abc': 'cde' } }),
      functionName: 'testdb-athena-postgres-connector',
      memorySize: 512,
      timeout: cdk.Duration.minutes(1)
    });
    return connector;
  }

  private configureDbClusterToAllowConnectionsFromLambda(connector: lambda.Function) {
    // same account demo, if this was cross account we would have to ingress using the CIDR of the VPC hosting the RDS db
    const clusterSecurityGroup = ec2.SecurityGroup.fromSecurityGroupId(this, 'import-rds-cluster-security-group', 'id-of-database-security-group');
    clusterSecurityGroup.connections.allowFrom(connector, ec2.Port.tcp(5432));
  }
}
