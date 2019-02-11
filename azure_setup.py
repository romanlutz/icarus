import os
import pickle
from datetime import datetime
from azure_helpers import storage, authentication
from icarus.run import _validate_settings
from icarus.util import Settings
import constants

EXPERIMENT_QUEUE_KEY = 'EXPERIMENT_QUEUE'

def _create_settings_with_one_experiment(settings, experiment_index):
    copied_settings = settings.clone()
    all_experiments = settings.get(EXPERIMENT_QUEUE_KEY)
    copied_settings.set(EXPERIMENT_QUEUE_KEY, all_experiments[experiment_index])
    return copied_settings

credentials = authentication.get_sp_credentials(
    name=constants.SP_NAME,
    password=constants.SP_PASSWORD,
    tenant=constants.SP_TENANT
)

# set up blob for experiments
block_blob_service = storage.get_block_blob_service(
    credentials,
    constants.SUBSCRIPTION_ID,
    constants.RESOURCE_GROUP_NAME,
    constants.STORAGE_ACCOUNT_NAME)
block_blob_service.create_container(constants.CONTAINER_NAME)

# TODO check blob to see if any experiments already exist and ask whether they should be deleted first

# Azure Table works with PartitionKey and RowKey for indexing.
# For our purposes PartitionKey is set to the trace name and
# RowKey is set to the name of the caching algorithm.
settings = Settings()
settings.read_from('config.py')
# Validate settings
_validate_settings(settings, freeze=True)

for experiment_index, experiment in enumerate(settings.get(EXPERIMENT_QUEUE_KEY)):
    single_experiment_settings = _create_settings_with_one_experiment(settings, experiment_index)
    now = datetime.now()
    file_name = '{}-{}-{}-{}.pickle'.format(now.year, now.month, now.day, experiment_index)
    with open(file_name, 'wb') as settings_file:
        pickle.dump(single_experiment_settings, settings_file)
    block_blob_service.create_blob_from_path(constants.CONTAINER_NAME, file_name, os.path.abspath(file_name))
