from aws_cdk import (
    Stack, aws_iam as iam, aws_glue as glue, aws_s3 as s3
)
from constructs import Construct

class GlueStack(Stack):
    def __init__(self, scope: Construct, id: str, redshift_cluster, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.bucket = s3.Bucket(self, "RawDataBucket")

        glue_role = iam.Role(self, "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftAllCommandsFullAccess")
            ]
        )

        connection = glue.CfnConnection(self, "RedshiftConnection",
            catalog_id=self.account,
            connection_input={
                "name": "redshift-connection",
                "connection_type": "JDBC",
                "connection_properties": {
                    "JDBC_CONNECTION_URL": redshift_cluster.cluster_endpoint.socket_address,
                    "USERNAME": "admin",
                    "PASSWORD": redshift_cluster.secret.secret_value_from_json("password").unsafe_unwrap()
                },
                "physicalConnectionRequirements": {
                    "SubnetId": redshift_cluster.vpc.private_subnets[0].subnet_id,
                    "SecurityGroupIdList": [sg.security_group_id for sg in redshift_cluster.connections.security_groups]
                }
            }
        )

        self.glue_role = glue_role
        self.glue_connection_name = connection.ref
        self.bucket_name = self.bucket.bucket_name