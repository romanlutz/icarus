from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlockBlobService, PublicAccess


def get_storage_keys(credentials,
                     subscription_id,
                     resource_group_name,
                     storage_account_name):
    storage_client = StorageManagementClient(credentials, subscription_id)
    storage_keys = storage_client.storage_accounts.list_keys(resource_group_name, storage_account_name)
    return {v.key_name: v.value for v in storage_keys.keys}

def get_block_blob_service(credentials,
                           subscription_id,
                           resource_group_name,
                           storage_account_name):
    storage_keys = get_storage_keys(
        credentials,
        subscription_id,
        resource_group_name,
        storage_account_name)
    return BlockBlobService(account_name=storage_account_name, account_key=storage_keys['key1'])
