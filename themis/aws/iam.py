import boto3
import json
import logging

from mypy_boto3_iam import IAMClient

logger = logging.getLogger(__name__)


def create_service_role_if_not_exist(
        role_name: str,
        services: list[str],
        policy_arns: list[str],
        description: str = None,
        region_name: str = None,
        iam: IAMClient = None):
    if iam is None:
        iam = boto3.client("iam", region_name=region_name)

    try:
        iam.get_role(RoleName=role_name)
    except iam.exceptions.NoSuchEntityException:
        assume_role_doc = _get_trust_relationship_for_service(services)

        if description is None:
            description = role_name + "created by Analytics Benchmark Service"

        logger.info(f"IAM role {role_name} not found, creating new one for service {services} with "
                    f"description {description}, policies {policy_arns}")
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_doc),
            Description=description,
            MaxSessionDuration=43200) # 12 hours, max allowed duration
        for policy_arn in policy_arns:
            iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)


def _get_trust_relationship_for_service(services: list[str]):
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": [f"{service}.amazonaws.com" for service in services]
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
