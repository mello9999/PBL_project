import pandas as pd
import boto3
import json

# Load DWH Params from a file
import configparser
config_infra = configparser.ConfigParser()
config_infra.read_file(open('config/config.cfg'))

KEY                    = config_infra.get('AWS','KEY')
SECRET                 = config_infra.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config_infra.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config_infra.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config_infra.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config_infra.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config_infra.get("DWH","DWH_DB")
DWH_DB_USER            = config_infra.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config_infra.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config_infra.get("DWH","DWH_PORT")

print(pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT]
             }))

###


# Create clients for IAM, S3 and Redshift
import boto3

redshift = boto3.client("redshift", region_name = "us-west-2", aws_access_key_id = KEY, aws_secret_access_key = SECRET)


# Delete the created resources
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)
