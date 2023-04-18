import os

import boto3
import json
import logging
import toml

import aws_cdk as cdk
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_deployment as s3deploy
import aws_cdk.aws_iam as iam
import aws_cdk.aws_mwaa as mwaa
import aws_cdk.aws_ecs as ecs
import aws_cdk.aws_logs as logs

from aws_cdk import Stack, CfnOutput
from constructs import Construct
from aws_cdk.aws_ecr_assets import DockerImageAsset
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class AnalyticsBenchmarkServiceStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # inputs
        if not (vpc_cidr := self.node.try_get_context('VpcCidr')):
            vpc_cidr = '10.0.0.0/16'

        if not (bucket_name := self.node.try_get_context('BucketName')):
            bucket_name = f'abs-bucket-{self.account}-{self.region}'

        if not (mwaa_name := self.node.try_get_context('MwaaName')):
            mwaa_name = f'abs-mwaa-{self.account}-{self.region}'

        if not (mwaa_version := self.node.try_get_context('MwaaVersion')):
            mwaa_version = '2.2.2'

        if not (mwaa_workers := self.node.try_get_context('MwaaWorkers')):
            mwaa_workers = 25
        else:
            mwaa_workers = int(mwaa_workers)

        if not (mwaa_class := self.node.try_get_context('MwaaClass')):
            mwaa_class = 'mw1.small'

        if not (ecs_cluster_name := self.node.try_get_context('EcsClusterName')):
            ecs_cluster_name = f'abs-ecs-{self.account}-{self.region}'

        # stack definition
        vpc = ec2.Vpc(
            scope=self,
            id='Vpc',
            max_azs=2,
            ip_addresses=ec2.IpAddresses.cidr(vpc_cidr),
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(subnet_type=ec2.SubnetType.PUBLIC, name='Public', cidr_mask=18),
                ec2.SubnetConfiguration(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS, name='Private', cidr_mask=18)
            ],
            gateway_endpoints={
                "S3": ec2.GatewayVpcEndpointOptions(
                    service=ec2.GatewayVpcEndpointAwsService.S3
                )
            }
        )

        bucket = s3.Bucket(
            scope=self,
            id='Bucket',
            bucket_name=bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True
        )

        s3deploy.BucketDeployment(
            scope=self,
            id='BucketDeployment',
            sources=[s3deploy.Source.asset(f'dist/abs.zip')],
            destination_bucket=bucket,
            destination_key_prefix='abs',
            retain_on_delete=False
        )

        mwaa_role = iam.Role(
            scope=self,
            id='MwaaRole',
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal('airflow.amazonaws.com'),
                iam.ServicePrincipal('airflow-env.amazonaws.com'),
            ),
            inline_policies={
                'MwaaPolicyDocument': _get_mwaa_policy_document(self, bucket.bucket_arn, mwaa_name)
            }
        )

        security_group = ec2.SecurityGroup(
            scope=self,
            id='MwaaSecurityGroup',
            vpc=vpc)
        security_group.connections.allow_internally(port_range=ec2.Port.all_traffic())

        network_config = mwaa.CfnEnvironment.NetworkConfigurationProperty(
            security_group_ids=[security_group.security_group_id],
            subnet_ids=[subnet.subnet_id for subnet in vpc.private_subnets])

        mwaa_env = mwaa.CfnEnvironment(
            scope=self,
            id='MwaaEnvironment',
            name=mwaa_name,
            airflow_version=mwaa_version,
            environment_class=mwaa_class,
            airflow_configuration_options={"core.dag_run_conf_overrides_params": "True"},
            execution_role_arn=mwaa_role.role_arn,
            logging_configuration=_get_mwaa_logging_config(),
            max_workers=mwaa_workers,
            network_configuration=network_config,
            source_bucket_arn=bucket.bucket_arn,
            dag_s3_path='abs',
            webserver_access_mode='PUBLIC_ONLY',
        )

        ecs_cluster = ecs.Cluster(
            scope=self,
            id='EcsCluster',
            cluster_name=ecs_cluster_name,
            enable_fargate_capacity_providers=True,
            vpc=vpc
        )

        task_definition = ecs.FargateTaskDefinition(
            scope=self,
            id='FargateTaskDefinition'
        )

        docker_image_asset = DockerImageAsset(
            scope=self,
            id='DockerImageAsset',
            directory='.',
            file='Dockerfile'
        )

        log_group = logs.LogGroup(
            scope=self,
            id='LogGroup'
        )

        task_definition.add_container(
            id='FargateTaskContainer',
            image=ecs.ContainerImage.from_docker_image_asset(docker_image_asset),
            logging=ecs.AwsLogDriver(
                log_group=log_group,
                stream_prefix='abs'
            ),
            environment={
                "EC2_PUBLIC_SUBNET_IDS": ",".join([s.subnet_id for s in vpc.public_subnets]),
                "EC2_PRIVATE_SUBNET_IDS": ",".join([s.subnet_id for s in vpc.private_subnets]),
                "S3_BUCKET_NAME": bucket.bucket_name
            }
        )

        # outputs
        CfnOutput(self, 'VpcId', value=vpc.vpc_id)
        CfnOutput(self, 'VpcArn', value=vpc.vpc_arn)
        for i in range(len(vpc.public_subnets)):
            CfnOutput(self, f'VpcPublicSubnet{i}Id', value=vpc.public_subnets[i].subnet_id)
            CfnOutput(self, f'VpcPublicSubnet{i}Az', value=vpc.public_subnets[i].availability_zone)
        for i in range(len(vpc.private_subnets)):
            CfnOutput(self, f'VpcPrivateSubnet{i}Id', value=vpc.private_subnets[i].subnet_id)
            CfnOutput(self, f'VpcPrivateSubnet{i}Az', value=vpc.private_subnets[i].availability_zone)
        CfnOutput(self, 'BucketName', value=bucket.bucket_name)
        CfnOutput(self, 'BucketArn', value=bucket.bucket_arn)
        CfnOutput(self, 'LogGroupName', value=log_group.log_group_name)
        CfnOutput(self, 'FargateTaskDefinitionArn', value=task_definition.task_definition_arn)
        CfnOutput(self, 'EcsClusterName', value=ecs_cluster.cluster_name)
        CfnOutput(self, 'EcsClusterArn', value=ecs_cluster.cluster_arn)
        CfnOutput(self, 'MwaaRoleName', value=mwaa_role.role_name)
        CfnOutput(self, 'MwaaRoleArn', value=mwaa_role.role_arn)
        CfnOutput(self, 'MwaaEnvironmentName', value=mwaa_env.name)
        CfnOutput(self, 'MwaaSecurityGroupId', value=security_group.security_group_id)
        CfnOutput(self, 'EcrRepositoryName', value=docker_image_asset.repository.repository_name)
        CfnOutput(self, 'EcrRepositoryArn', value=docker_image_asset.repository.repository_arn)
        CfnOutput(self, 'EcrRepositoryUri', value=docker_image_asset.repository.repository_uri)
        CfnOutput(self, 'EcrImageTag', value=docker_image_asset.image_tag)
        CfnOutput(self, 'EcrImageUri', value=docker_image_asset.image_uri)


def _get_mwaa_policy_document(stack, bucket_arn, mwaa_name):
    return iam.PolicyDocument(
        statements=[
            iam.PolicyStatement(
                actions=['airflow:PublishMetrics'],
                effect=iam.Effect.ALLOW,
                resources=[f'arn:aws:airflow:{stack.region}:{stack.account}:environment/{mwaa_name}'],
            ),
            iam.PolicyStatement(
                actions=[
                    's3:ListAllMyBuckets'
                ],
                effect=iam.Effect.DENY,
                resources=[
                    f'{bucket_arn}/*',
                    f'{bucket_arn}'
                ],
            ),
            iam.PolicyStatement(
                actions=[
                    's3:*'
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f'{bucket_arn}/*',
                    f'{bucket_arn}'
                ],
            ),
            iam.PolicyStatement(
                actions=[
                    'logs:CreateLogStream',
                    'logs:CreateLogGroup',
                    'logs:PutLogEvents',
                    'logs:GetLogEvents',
                    'logs:GetLogRecord',
                    'logs:GetLogGroupFields',
                    'logs:GetQueryResults',
                    'logs:DescribeLogGroups'
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f'arn:aws:logs:{stack.region}:{stack.account}:log-group:airflow-{mwaa_name}-*'],
            ),
            iam.PolicyStatement(
                actions=[
                    'logs:DescribeLogGroups',
                    'logs:GetLogEvents'
                ],
                effect=iam.Effect.ALLOW,
                resources=['*'],
            ),
            iam.PolicyStatement(
                actions=[
                    'ecs:*'
                ],
                effect=iam.Effect.ALLOW,
                resources=['*'],
            ),
            iam.PolicyStatement(
                actions=[
                    'iam:GetRole',
                    'iam:PassRole'
                ],
                effect=iam.Effect.ALLOW,
                resources=['*'],
            ),
            iam.PolicyStatement(
                actions=[
                    'sqs:ChangeMessageVisibility',
                    'sqs:DeleteMessage',
                    'sqs:GetQueueAttributes',
                    'sqs:GetQueueUrl',
                    'sqs:ReceiveMessage',
                    'sqs:SendMessage'
                ],
                effect=iam.Effect.ALLOW,
                resources=[f'arn:aws:sqs:{stack.region}:*:airflow-celery-*'],
            ),
            iam.PolicyStatement(
                actions=[
                    'kms:Decrypt',
                    'kms:DescribeKey',
                    'kms:GenerateDataKey*',
                    'kms:Encrypt',
                    'kms:PutKeyPolicy'
                ],
                effect=iam.Effect.ALLOW,
                resources=['*'],
                conditions={
                    'StringEquals': {
                        'kms:ViaService': [
                            f'sqs.{stack.region}.amazonaws.com',
                            f's3.{stack.region}.amazonaws.com',
                        ]
                    }
                },
            ),
        ]
    )


def _get_mwaa_logging_config():
    return mwaa.CfnEnvironment.LoggingConfigurationProperty(
        dag_processing_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
            enabled=True,
            log_level='INFO'
        ),
        task_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
            enabled=True,
            log_level='INFO'
        ),
        worker_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
            enabled=True,
            log_level='INFO'
        ),
        scheduler_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
            enabled=True,
            log_level='INFO'
        ),
        webserver_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
            enabled=True,
            log_level='INFO'
        )
    )


def _set_ecr_lifecycle_policy(stack_name: str) -> None:
    """
    Hack to make sure we set a lifecycle policy for the ECR repository.
    Overall, CDK and ECR integration is messed up.
    CDK team trys to deprecate all references to repository_name,
    but user still needs to manage lifecycle of ECR images.
    See https://github.com/aws/aws-cdk/issues/8483 for more details.

    We run this after app.synth(), so after the stack is initialized,
    the ECR repository will always have the right lifecycle policy.

    Args:
        stack_name: name of the stack to get ECR repository associated
    """
    cloudformation = boto3.client('cloudformation')
    try:
        stack_outputs = cloudformation.describe_stacks(StackName=stack_name)['Stacks'][0]['Outputs']
    except ClientError:
        logger.warning(f"Cannot find CloudFormation stack {stack_name}, skip setting ECR repository lifecycle policy")
        return

    repository_name = next(filter(
        lambda out: out['OutputKey'] == 'EcrRepositoryName', stack_outputs))['OutputValue']

    boto3.client('ecr').put_lifecycle_policy(
        repositoryName=repository_name,
        lifecyclePolicyText=json.dumps({
            'rules': [{
                'rulePriority': 1,
                'description': 'Expire all images except the last 3',
                'selection': {
                    'tagStatus': 'any',
                    'countType': 'imageCountMoreThan',
                    'countNumber': 3
                },
                'action': {
                    'type': 'expire'
                }
            }]}))


def _create_zip_from_tarball_dist() -> None:
    with open('pyproject.toml') as project_file:
        pyproject = toml.loads(project_file.read())
        version = pyproject['tool']['poetry']['version']
        logger.warning(f"Identified abs version as {version}")
        prefix = f"abs-{version}"

        if not os.path.isfile(f"dist/{prefix}.tar.gz"):
            logger.warning("Cannot find abs tarball, Running 'poetry build' to build abs first")
            os.system("poetry build")

        os.system(f"cd dist && tar xzf {prefix}.tar.gz && zip -r abs.zip {prefix} && rm -rf {prefix}")


if __name__ == '__main__':
    _create_zip_from_tarball_dist()
    app = cdk.App()
    if not (name := app.node.try_get_context('StackName')):
        name = 'AnalyticsBenchmarkService'

    AnalyticsBenchmarkServiceStack(scope=app, construct_id=name)
    app.synth()
    _set_ecr_lifecycle_policy(name)
