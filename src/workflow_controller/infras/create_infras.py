import pandas as pd
import boto3
import json

import configparser
config_infra = configparser.ConfigParser()
config_infra.read_file(open('config/config.cfg'))

KEY                    = config_infra.get('AWS','LOGIN')
SECRET                 = config_infra.get('AWS','PASSWORD')

DWH_CLUSTER_TYPE       = config_infra.get("REDSHIFT","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config_infra.get("REDSHIFT","DWH_NUM_NODES")
DWH_NODE_TYPE          = config_infra.get("REDSHIFT","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config_infra.get("REDSHIFT","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config_infra.get("REDSHIFT","DWH_DB")
DWH_DB_USER            = config_infra.get("REDSHIFT","DWH_DB_USER")
DWH_DB_PASSWORD        = config_infra.get("REDSHIFT","DWH_DB_PASSWORD")
DWH_PORT               = config_infra.get("REDSHIFT","DWH_PORT")

print(pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT]
             }))

###


# Create clients for Redshift
import boto3

redshift = boto3.client("redshift", region_name = "us-west-2", aws_access_key_id = KEY, aws_secret_access_key = SECRET)

###


# Create a RedShift Cluster
try:
    response = redshift.create_cluster(        
        # Add parameters for hardware
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        

        # Add parameters for identifiers & credentials
        DBName = DWH_DB,
        ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
        MasterUsername = DWH_DB_USER,
        MasterUserPassword = DWH_DB_PASSWORD
    )

except Exception as e:
    print(e)

###


# Describe the cluster to see its status
import itertools
import threading
import time
import sys

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

done = False
# The animation funation
def animate():
    for c in itertools.cycle(['|', '/', '-', '\\']):
        if done:
            break
        sys.stdout.write('\rcreating ' + c )
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\ravailable!     '+'\n')

t = threading.Thread(target=animate)
t.start()

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
while myClusterProps['ClusterStatus'] != 'available':
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    time.sleep(1)

print()
print(prettyRedshiftProps(myClusterProps))

done = True
t.join()

###

# Defined endpoint and arn
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)

###


# Config dwh.cfg
config_dwh = configparser.ConfigParser()
config_dwh.read_file(open('config/config.cfg'))

config_dwh["AWS"] = {
    "CONN_ID":config_dwh.get('AWS','CONN_ID'),
    "CONN_TYPE":config_dwh.get('AWS','CONN_TYPE'),
    "LOGIN": KEY,
    "PASSWORD": SECRET
}

config_dwh["REDSHIFT"] = {
    "CONN_ID":config_dwh.get('REDSHIFT','CONN_ID'),
    "CONN_TYPE":config_dwh.get('REDSHIFT','CONN_TYPE'),
    "DWH_CLUSTER_TYPE":DWH_CLUSTER_TYPE,
    "DWH_NUM_NODES":DWH_NUM_NODES, 
    "DWH_NODE_TYPE":DWH_NODE_TYPE,
    "DWH_CLUSTER_IDENTIFIER":DWH_CLUSTER_IDENTIFIER,
    "DB_NAME": DWH_DB,
    "DB_USER": DWH_DB_USER,
    "DB_PASSWORD": DWH_DB_PASSWORD,
    "DB_PORT": DWH_PORT,
    "HOST": DWH_ENDPOINT
}

config_dwh["S3"] = {
    "S3_SONG_KEY":config_dwh.get('S3','S3_SONG_KEY'),
    "S3_LOG_KEY":config_dwh.get('S3','S3_LOG_KEY'),
    "S3_BUCKET": config_dwh.get('S3','S3_BUCKET')
}

#Write the above sections to dwh.cfg file
with open('config/config.cfg', 'w') as conf:
    config_dwh.write(conf)
    
###