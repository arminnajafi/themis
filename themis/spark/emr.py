import logging

import boto3

import themis.aws.s3 as themis_s3
import themis.aws.emr as themis_emr

from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)

EMR_EC2_SF_CLUSTER_NAME_DEFAULT = "THEMIS_Spark_EMR_EC2_ScaleFactor"
EMR_EC2_SF_NODE_TYPE_DEFAULT = "r5d.4xlarge"

EMR_EC2_DPU_CLUSTER_NAME_DEFAULT = "THEMIS_Spark_EMR_EC2_DPU"
EMR_EC2_DPU_NODE_TYPE = "m6g.xlarge"
EMR_EC2_DPU_BOOTSTRAP_ACTION_FILE_PATH = "emr-ec2/dpu_bootstrap.sh"

EMR_EC2_VERSION_DEFAULT = "6.9.0"
EMR_IDLE_TIME_SECONDS_DEFAULT = "900"
EMR_HIVE_GLUE_CONFIG = {
    'Classification': 'spark-hive-site',
    'Properties': {
        'hive.metastore.client.factory.class':
            'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory',
        "hive.exec.dynamic.partition": "true",
        "hive.exec.dynamic.partition.mode": "nonstrict",
        "hive.exec.max.dynamic.partitions": "999999"
    }
}
EMR_WAIT_DEFAULT = "True"

IAM_JOB_FLOW_ROLE_NAME_DEFAULT = 'EMR_EC2_DefaultRole'
IAM_SERVICE_ROLE_DEFAULT = 'EMR_DefaultRole'
IAM_AUTO_SCALING_ROLE_DEFAULT = 'EMR_AutoScaling_DefaultRole'


def create_emr_ec2_sf_cluster(
        scale_factor: str = None,
        name: str = EMR_EC2_SF_CLUSTER_NAME_DEFAULT,
        emr_version: str = EMR_EC2_VERSION_DEFAULT,
        emr_idle_time_seconds: str = EMR_IDLE_TIME_SECONDS_DEFAULT,
        s3_log_uri: str = None,
        ec2_node_type: str = EMR_EC2_SF_NODE_TYPE_DEFAULT,
        ec2_subnet_ids: str = None,
        ec2_key_pair_name: str = None,
        iam_job_flow_role_name: str = IAM_JOB_FLOW_ROLE_NAME_DEFAULT,
        iam_service_role_name: str = IAM_SERVICE_ROLE_DEFAULT,
        iam_auto_scaling_role_name: str = IAM_AUTO_SCALING_ROLE_DEFAULT,
        wait: str = EMR_WAIT_DEFAULT,
        region_name: str = None) -> str:
    logger.info('Creating scale factor based EMR cluster')

    return themis_emr.create_cluster_with_retry(
        name=name if name is not None else EMR_EC2_SF_CLUSTER_NAME_DEFAULT,
        ec2_core_node_count=_calc_core_node_count(int(scale_factor) if scale_factor is not None else None),
        ec2_node_type=ec2_node_type,
        emr_version=emr_version,
        emr_bootstrap_actions=[],
        emr_configurations=[
            {
                'Classification': 'spark',
                'Properties': {
                    'maximizeResourceAllocation': 'true'
                }
            },
            {
                'Classification': 'spark-defaults',
                'Properties': {
                    'spark.dynamicAllocation.enabled': 'false',
                }
            },
            EMR_HIVE_GLUE_CONFIG
        ],
        emr_applications=[
            {'Name': 'Spark'},
            {'Name': 'JupyterEnterpriseGateway'},
            {'Name': 'Ganglia'}
        ],
        s3_log_uri=s3_log_uri,
        ec2_subnet_ids=ec2_subnet_ids.split(",") if ec2_subnet_ids is not None else None,
        ec2_key_pair_name=ec2_key_pair_name,
        iam_job_flow_role_name=iam_job_flow_role_name,
        iam_service_role_name=iam_service_role_name,
        iam_auto_scaling_role_name=iam_auto_scaling_role_name,
        emr_idle_time_seconds=int(emr_idle_time_seconds),
        wait=True if wait is not None and wait.lower() == "true" else False,
        region_name=region_name)


def create_emr_ec2_dpu_cluster(
        max_dpu: str,
        min_dpu: str,
        init_dpu: str,
        s3_bucket_name: str,
        name: str = EMR_EC2_DPU_CLUSTER_NAME_DEFAULT,
        emr_version: str = EMR_EC2_VERSION_DEFAULT,
        emr_idle_time_seconds: str = EMR_IDLE_TIME_SECONDS_DEFAULT,
        s3_log_uri: str = None,
        ec2_subnet_ids: str = None,
        ec2_key_pair_name: str = None,
        iam_job_flow_role_name: str = IAM_JOB_FLOW_ROLE_NAME_DEFAULT,
        iam_service_role_name: str = IAM_SERVICE_ROLE_DEFAULT,
        iam_auto_scaling_role_name: str = IAM_AUTO_SCALING_ROLE_DEFAULT,
        wait: str = EMR_WAIT_DEFAULT,
        region_name: str = None,
        s3: S3Client = None) -> str:
    logger.info(f'Creating DPU based EMR cluster')

    if not s3:
        s3 = boto3.client("s3", region_name=region_name)

    bootstrap_file_uri = themis_s3.upload_resource(
        path=EMR_EC2_DPU_BOOTSTRAP_ACTION_FILE_PATH,
        bucket_name=s3_bucket_name,
        upload_folder_path=name,
        s3=s3)

    return themis_emr.create_cluster_with_retry(
        name=name,
        ec2_core_node_count=int(max_dpu) + 1,  # One extra DPU for driver
        ec2_node_type=EMR_EC2_DPU_NODE_TYPE,
        emr_version=emr_version,
        emr_bootstrap_actions=[
            {
                'Name': 'install Python libraries',
                'ScriptBootstrapAction': {
                    'Path': bootstrap_file_uri
                }
            }
        ],
        emr_configurations=[
            {
                'Classification': 'spark-defaults',
                'Properties': {
                    'spark.executor.cores': '4',
                    'spark.dynamicAllocation.minExecutors': f'{min_dpu}',
                    'spark.dynamicAllocation.initialExecutors': f'{init_dpu}',
                    'spark.dynamicAllocation.maxExecutors': f'{max_dpu}',
                    'spark.dynamicAllocation.enabled': 'true',
                    'spark.dynamicAllocation.shuffleTracking.enabled': 'true',
                    'spark.shuffle.service.enabled': 'false'
                }
            },
            EMR_HIVE_GLUE_CONFIG
        ],
        emr_applications=[
            {'Name': 'Spark'},
            {'Name': 'JupyterHub'}
        ],
        s3_log_uri=s3_log_uri,
        ec2_subnet_ids=ec2_subnet_ids.split(",") if ec2_subnet_ids is not None else None,
        ec2_key_pair_name=ec2_key_pair_name,
        iam_job_flow_role_name=iam_job_flow_role_name,
        iam_service_role_name=iam_service_role_name,
        iam_auto_scaling_role_name=iam_auto_scaling_role_name,
        emr_idle_time_seconds=int(emr_idle_time_seconds),
        wait=True if wait is not None and wait.lower() == "true" else False,
        region_name=region_name
    )


def _calc_core_node_count(scale_factor: int):
    if scale_factor is None:
        return 5
    elif scale_factor < 100:
        return 10
    elif scale_factor < 1000:
        return 20
    else:
        return 30
