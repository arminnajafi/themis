import logging

import backoff
import boto3
import random

import themis.airflow.xcom as xcom
import themis.aws.iam as themis_iam

from mypy_boto3_iam import IAMClient
from mypy_boto3_emr import EMRClient

logger = logging.getLogger(__name__)

EMR_SCALE_DOWN_BEHAVIOR_DEFAULT = 'TERMINATE_AT_TASK_COMPLETION'


def create_cluster(
        name: str,
        ec2_core_node_count: int,
        ec2_node_type: str,
        ec2_subnet_ids: list[str],
        ec2_key_pair_name: str,
        emr_version: str,
        emr_idle_time_seconds: int,
        emr_bootstrap_actions: list[dict],
        emr_configurations: list[dict],
        emr_applications: list[dict],
        s3_log_uri: str,
        iam_job_flow_role_name: str,
        iam_service_role_name: str,
        iam_auto_scaling_role_name: str,
        wait: bool,
        region_name: str = None,
        emr: EMRClient = None,
        iam: IAMClient = None) -> str:
    logger.info('Creating scale factor based EMR cluster')
    if emr is None:
        emr = boto3.client("emr", region_name=region_name)
    if iam is None:
        iam = boto3.client("iam", region_name=region_name)

    _create_emr_roles_if_not_exist(
        job_flow_role_name=iam_job_flow_role_name,
        service_role_name=iam_service_role_name,
        auto_scaling_role_name=iam_auto_scaling_role_name,
        iam=iam)
    instances = {
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'InstanceGroups': [
            {
                'Name': 'Master - 1',
                'InstanceRole': 'MASTER',
                'InstanceType': ec2_node_type,
                'InstanceCount': 1
            },
            {
                'Name': 'Core - 2',
                'InstanceRole': 'CORE',
                'InstanceType': ec2_node_type,
                'InstanceCount': ec2_core_node_count
            }
        ]
    }

    # we randomly pick the subnet to initialize the cluster
    # because one AZ might not have enough capacity to get the requested number of nodes,
    # and we don't know which AZ to use either
    # randomly picking one is our best effort to get the cluster size we want
    # if failed, retry will be triggered to get pick another one through _create_cluster_with_retry
    if ec2_subnet_ids is not None:
        instances["Ec2SubnetId"] = random.choice(ec2_subnet_ids)

    if ec2_key_pair_name is not None:
        instances["Ec2KeyName"] = ec2_key_pair_name

    auto_termination_policy = {
        'IdleTimeout': int(emr_idle_time_seconds)
    }

    if s3_log_uri is None:
        response = emr.run_job_flow(
            Name=name,
            ReleaseLabel=f'emr-{emr_version}',
            Instances=instances,
            BootstrapActions=emr_bootstrap_actions,
            Configurations=emr_configurations,
            Applications=emr_applications,
            JobFlowRole=iam_job_flow_role_name,
            ServiceRole=iam_service_role_name,
            AutoScalingRole=iam_auto_scaling_role_name,
            ScaleDownBehavior=EMR_SCALE_DOWN_BEHAVIOR_DEFAULT,
            AutoTerminationPolicy=auto_termination_policy
        )
    else:
        response = emr.run_job_flow(
            Name=name,
            LogUri=s3_log_uri,
            ReleaseLabel=f'emr-{emr_version}',
            Instances=instances,
            BootstrapActions=emr_bootstrap_actions,
            Configurations=emr_configurations,
            Applications=emr_applications,
            JobFlowRole=iam_job_flow_role_name,
            ServiceRole=iam_service_role_name,
            AutoScalingRole=iam_auto_scaling_role_name,
            ScaleDownBehavior=EMR_SCALE_DOWN_BEHAVIOR_DEFAULT,
            AutoTerminationPolicy=auto_termination_policy
        )

    cluster_id = response.get('JobFlowId')
    if wait:
        _wait_for_cluster_ready(emr, cluster_id)

    xcom.log_cluster_id(logger, cluster_id)
    return cluster_id


@backoff.on_exception(
    wait_gen=backoff.constant,
    exception=Exception,
    max_time=3600 * 5,
    interval=3 * 60,
    jitter=None)
def create_cluster_with_retry(**kwargs) -> str:
    return create_cluster(**kwargs)


def _create_emr_roles_if_not_exist(
        job_flow_role_name: str,
        service_role_name: str,
        auto_scaling_role_name: str,
        iam: IAMClient):
    themis_iam.create_service_role_if_not_exist(
        role_name=job_flow_role_name,
        services=["ec2"],
        policy_arns=[
            "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role",
            "arn:aws:iam::aws:policy/AmazonAthenaFullAccess",
            "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"],
        iam=iam)

    themis_iam.create_service_role_if_not_exist(
        role_name=service_role_name,
        services=["elasticmapreduce"],
        policy_arns=[
            "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole",
            "arn:aws:iam::aws:policy/AmazonSQSFullAccess",
            "arn:aws:iam::aws:policy/AmazonSNSFullAccess"],
        iam=iam)

    themis_iam.create_service_role_if_not_exist(
        role_name=auto_scaling_role_name,
        services=["elasticmapreduce", "application-autoscaling"],
        policy_arns=["arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforAutoScalingRole"],
        iam=iam)


def _wait_for_cluster_ready(emr, cluster_id):
    logger.info(f'Waiting for EMR cluster to start running - cluster_id: {cluster_id}')
    waiter = emr.get_waiter('cluster_running')
    waiter.wait(
        ClusterId=cluster_id,
        WaiterConfig={
            'Delay': 3 * 60,  # The amount of time in seconds to wait between attempts
            'MaxAttempts': 5  # The maximum number of attempts to be made
        }
    )
