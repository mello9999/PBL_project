import os
from azure.mgmt.rdbms.postgresql.models import *
from azure.mgmt.resource import SubscriptionClient
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.rdbms.postgresql import PostgreSQLManagementClient

class azureRDMS():
    def __init__(self):
        self.SUBSCRIPTION_ID = "d1d6c02e-83ed-4898-8e84-20570565e603"
        self.RESOURCE_GROUP = "PBL_resource"

        self.tenant_id = "6f4432dc-20d2-441d-b1db-ac3380ba633d"
        self.client_id = "c5adeff9-7861-4ec8-b233-93dc5fc8fb91"
        self.client_secret = "94218968-bd01-4b92-b596-fd1ed28b2426"

        self.SERVER = "pblserver1"                       # SET_YOUR_SERVER_NAME
        self.ADMIN_USER = "pbldb1"                       # SET_YOUR_DATABASE_USERNAME
        self.ADMIN_PASSWORD = "Eye123456789."            # SET_YOUR_DATABASE_PASSWORD
        self.LOCATION = "southeastasia"

        self.credential = ServicePrincipalCredentials(
            tenant=self.tenant_id,
            client_id=self.client_id,
            secret=self.client_secret
        )
        
        self.client = PostgreSQLManagementClient(
            credentials=self.credential,
            subscription_id=self.SUBSCRIPTION_ID
        )
    
    def create(self):

        server_creation_poller = self.client.servers.create(
            self.RESOURCE_GROUP,
            self.SERVER,
            ServerForCreate(
                properties=ServerPropertiesForDefaultCreate(
                    administrator_login=self.ADMIN_USER,
                    administrator_login_password=self.ADMIN_PASSWORD,
                    version=ServerVersion.one_one,
                    storage_profile=StorageProfile(
                        storage_mb=51200
                    )
                ),
                location=self.LOCATION,
                sku=Sku(
                    name="GP_Gen5_2"
                )
            )
        )

        server = server_creation_poller.result()
        print("Create done")

    def delete(self):
        server_delete_poller = self.client.servers.delete(
            self.RESOURCE_GROUP,
            self.SERVER,
        )

        server = server_delete_poller.result()
        print("Delete done")