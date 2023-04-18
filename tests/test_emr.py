import boto3
import moto
import themis.aws.emr as abs_emr


@moto.mock_emr
@moto.mock_iam
def test_create_default_cluster():
    emr = boto3.client("emr", region_name="us-east-1")
    iam = boto3.client("iam", region_name="us-east-1")
    cluster_id = abs_emr.create_cluster(
        name="test",
        ec2_core_node_count=10,
        ec2_node_type="m4.xlarge",
        ec2_subnet_ids=["sn-1234"],
        ec2_key_pair_name="key",
        emr_version="6.1.0",
        emr_idle_time_seconds=10000,
        emr_bootstrap_actions=[],
        emr_configurations=[],
        emr_applications=[{"Name": "Spark"}],
        s3_log_uri="s3://test/path",
        iam_job_flow_role_name="role1",
        iam_service_role_name= "role2",
        iam_auto_scaling_role_name="role3",
        wait=False,
        emr=emr,
        iam=iam)

    cluster = emr.describe_cluster(ClusterId=cluster_id)['Cluster']
    assert cluster["Name"] == "test"
    assert cluster["Ec2InstanceAttributes"]["Ec2KeyName"] == "key"
    assert cluster["Ec2InstanceAttributes"]["Ec2SubnetId"] == "sn-1234"
    assert cluster["LogUri"] == "s3://test/path"
    assert cluster["ReleaseLabel"] == "emr-6.1.0"
    assert cluster["AutoTerminate"] is False
    assert cluster["TerminationProtected"] is False
    assert cluster["Applications"] == [{"Name": "Spark"}]
    assert cluster["ServiceRole"] == "role2"
    assert cluster["ServiceRole"] == "role2"
    assert cluster["AutoScalingRole"] == "role3"





