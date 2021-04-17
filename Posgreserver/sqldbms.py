import os
from azure.mgmt.rdbms.postgresql.models import *
from azure.mgmt.resource import SubscriptionClient
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.rdbms.postgresql import PostgreSQLManagementClient

class azureRDMS():
    def __init__(self):
        self.SUBSCRIPTION_ID = "YOUR_SUBSCRIPTION_ID"
        self.RESOURCE_GROUP = "YOUR_RESOURCE_GROUP"

        self.tenant_id = "YOUR_TENANT_ID"
        self.client_id = "YOUR_CLIENT_ID"
        self.client_secret = "YOUR_CLIENT_SECRET"

        self.SERVER = "SET_YOUR_SERVER_NAME"
        self.ADMIN_USER = "SET_YOUR_DATABASE_USERNAME"
        self.ADMIN_PASSWORD = "SET_YOUR_DATABASE_PASSWORD"
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