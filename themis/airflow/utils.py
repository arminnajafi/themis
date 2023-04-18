import logging
from typing import List

from themis.airflow.custom_ecs import CustomECSOperator
from themis.airflow.dags import get_ecs_task_definition_name, get_ecs_log_group_name, \
    get_ecs_log_stream_prefix_for_airflow, get_ecs_container_name

logger = logging.getLogger(__name__)


class AnalyticsBenchmarkCliTaskGenerator:

    def __init__(self, subnets):
        self.ecs_task_name = ATHENA_BENCHMARK_TASK
        self.cluster_name = ECS_CLUSTER
        self.subnets = subnets

    def get_task(self, airflow_task_name: str, cli_command: List[str]):
        task_definition = get_ecs_task_definition_name(task_name=self.ecs_task_name)
        awslogs_group = get_ecs_log_group_name(task_name=self.ecs_task_name)
        awslogs_stream_prefix = get_ecs_log_stream_prefix_for_airflow(task_name=self.ecs_task_name)
        logger.info(f'task_definition: {task_definition}')
        logger.info(f'awslogs_group: {awslogs_group}')
        logger.info(f'awslogs_stream_prefix: {awslogs_stream_prefix}')

        container_overrides = {"containerOverrides": [{
            'name': get_ecs_container_name(task_name=self.ecs_task_name),
            'command': cli_command
        }]}

        # Using a custom port to get around this bug: https://github.com/apache/airflow/pull/20814
        task = CustomECSOperator(
            task_id=airflow_task_name,
            overrides=container_overrides,
            cluster=self.cluster_name,
            task_definition=task_definition,
            launch_type='FARGATE',
            network_configuration={'awsvpcConfiguration': {'subnets': self.subnets}},
            awslogs_group=awslogs_group,
            awslogs_stream_prefix=awslogs_stream_prefix
        )
        return task

import os

from pathlib import Path
from typing import List

from themis.core.config import ATHENA_SPARK_WORKGROUP_PROD, ATHENA_SPARK_WORKGROUP_GAMMA, \
    ATHENA_SPARK_WORKGROUP_GAMMA_2
from themis.report.report_gen_utils import get_query_name_sort_tuple

CURRENT_DIR = Path(os.path.dirname(__file__))


def get_athena_endpoint_url(stage_name, region_name='us-east-1'):
    if stage_name == 'prod':
        return f'https://athena.{region_name}.amazonaws.com/'
    elif stage_name == 'gamma':
        return f'https://athena-webservice-preprod.{region_name}.amazonaws.com/'
    elif stage_name == 'beta':
        return f'https://athena-webservice-beta-ws.{region_name}.amazonaws.com/'
    else:
        raise ValueError(f'Invalid stage_name: {stage_name}')


def get_athena_spark_workgroup(stage_name):
    if stage_name == 'prod':
        return ATHENA_SPARK_WORKGROUP_PROD
    elif stage_name == 'gamma':
        return ATHENA_SPARK_WORKGROUP_GAMMA
    else:
        raise ValueError(f'Invalid stage_name: {stage_name}')


def get_athena_spark_workgroup_2(stage_name):
    if stage_name == 'prod':
        raise ValueError(f'Invalid stage_name: {stage_name}')
    elif stage_name == 'gamma':
        return ATHENA_SPARK_WORKGROUP_GAMMA_2
    else:
        raise ValueError(f'Invalid stage_name: {stage_name}')


def get_spark_tpcds_query_names():
    query_names = []
    dir_path = CURRENT_DIR.parent.parent.parent / 'resources' / 'tpcds_queries' / 'spark'
    for entry in dir_path.iterdir():
        if entry.is_file():
            assert entry.name.endswith('.sql')
            query_name = entry.name.split('.sql')[0]
            query_names.append(query_name)
    query_names.sort(key=lambda name: get_query_name_sort_tuple(name))
    return query_names


def merge_list_with_comma(_list: List):
    return ','.join(_list)
