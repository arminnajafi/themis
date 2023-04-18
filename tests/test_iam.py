import boto3
import moto
import json
import themis.aws.iam as abs_iam


@moto.mock_iam
def test_create_service_role_if_not_exist():
    iam = boto3.client("iam", region_name="us-east-1")
    services = ["athena", "glue"]
    policy_arns = [
        "arn:aws:iam::aws:policy/AmazonAthenaFullAccess",
        "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"]
    abs_iam.create_service_role_if_not_exist(
        role_name="TestRole",
        services=services,
        policy_arns=policy_arns,
        description="Test description",
        iam=iam)

    role = iam.get_role(RoleName="TestRole")["Role"]
    assert role["RoleName"] == "TestRole"
    assert json.dumps(role["AssumeRolePolicyDocument"]) == json.dumps(
        abs_iam._get_trust_relationship_for_service(services))
    assert role["Description"] == "Test description"
    assert role["MaxSessionDuration"] == 43200

    policies = iam.list_attached_role_policies(RoleName="TestRole")["AttachedPolicies"]
    assert set([policy["PolicyArn"] for policy in policies]) == set(policy_arns)


@moto.mock_iam
def test_skip_create_service_role_if_exists():
    iam = boto3.client("iam", region_name="us-east-1")
    iam.create_role(
        RoleName="TestRole",
        AssumeRolePolicyDocument=json.dumps(abs_iam._get_trust_relationship_for_service(["ec2"])),
        Description="desc",
        MaxSessionDuration=3600)

    abs_iam.create_service_role_if_not_exist(
        role_name="TestRole",
        services=["athena", "glue"],
        policy_arns=[
            "arn:aws:iam::aws:policy/AmazonAthenaFullAccess",
            "arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin"],
        description="Test description",
        iam=iam)

    role = iam.get_role(RoleName="TestRole")["Role"]
    assert role["RoleName"] == "TestRole"
    assert json.dumps(role["AssumeRolePolicyDocument"]) == json.dumps(
        abs_iam._get_trust_relationship_for_service(["ec2"]))
    assert role["Description"] == "desc"
    assert role["MaxSessionDuration"] == 3600

    policies = iam.list_attached_role_policies(RoleName="TestRole")["AttachedPolicies"]
    assert len(policies) == 0
