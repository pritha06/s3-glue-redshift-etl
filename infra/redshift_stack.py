from aws_cdk import (
    Stack, aws_ec2 as ec2, aws_redshift as redshift, aws_secretsmanager as secrets
)
from constructs import Construct

class RedshiftStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        vpc = ec2.Vpc(self, "RedshiftVPC", max_azs=2)

        redshift_sg = ec2.SecurityGroup(self, "RedshiftSG", vpc=vpc)
        redshift_sg.add_ingress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(5439))

        redshift_secret = secrets.Secret(self, "RedshiftSecret",
            secret_name="RedshiftAdminSecret",
            generate_secret_string={
                "secret_string_template": "{\"username\":\"admin\"}",
                "generate_string_key": "password",
                "exclude_characters": "@/\\\"'
            }
        )

        self.redshift_cluster = redshift.Cluster(self, "MyRedshift",
            vpc=vpc,
            master_user=redshift.Login(
                username="admin",
                password=redshift_secret.secret_value_from_json("password")
            ),
            default_database_name="analytics",
            number_of_nodes=2,
            node_type=redshift.NodeType.DC2_LARGE,
            security_groups=[redshift_sg],
            publicly_accessible=True
        )

        self.vpc = vpc
        self.redshift_secret = redshift_secret