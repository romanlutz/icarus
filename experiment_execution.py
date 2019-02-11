#%matplotlib inline
import numpy as np
import os
import matplotlib
import matplotlib.pyplot as plt
from random import choice
from string import ascii_lowercase
import time

from azure_helpers import storage, authentication
import azureml
from azureml.core import Workspace, Run, Experiment, Datastore

from icarus.run import run

import constants

from azureml.telemetry import set_diagnostics_collection
set_diagnostics_collection(send_diagnostics=True)


# create a random string to identify this execution instance/container
container_identity = ''.join(choice(ascii_lowercase) for i in range(12))

credentials = authentication.get_sp_credentials(
    name=constants.SP_NAME,
    password=constants.SP_PASSWORD,
    tenant=constants.SP_TENANT
)

auth = authentication.get_sp_auth(
    name=constants.SP_NAME,
    password=constants.SP_PASSWORD,
    tenant=constants.SP_TENANT
)

ws = Workspace.create(name=constants.WORKSPACE_NAME,
                      subscription_id=constants.SUBSCRIPTION_ID,    
                      resource_group=constants.RESOURCE_GROUP_NAME,
                      create_resource_group=False,
                      location='eastus2',
                      exist_ok=True,
                      auth=auth
                     )
print('Workspace name: ' + ws.name,
      'Azure region: ' + ws.location,
      'Subscription id: ' + ws.subscription_id,
      'Resource group: ' + ws.resource_group, sep = '\n')

script_folder = '.'
os.makedirs(script_folder, exist_ok=True)

experiment = Experiment(workspace=ws, name='cache-simulation')
experiment_run = experiment.start_logging()

# All the traces have been uploaded to this datastore ahead of time.
datastore = Datastore.get(ws, datastore_name='cachetraces')
datastore.download(target_path=os.path.join('resources'),
                   prefix='UMass_YouTube_traces')

block_blob_service = storage.get_block_blob_service(
    credentials,
    constants.SUBSCRIPTION_ID,
    constants.RESOURCE_GROUP_NAME,
    constants.STORAGE_ACCOUNT_NAME)

print("starting to check the blob for scenarios to execute")
while True:
    blobs = list(block_blob_service.list_blobs(constants.CONTAINER_NAME))
    for blob in blobs:
        print("found {}".format(blob.name))
        if os.path.splitext(blob.name)[1] == '.pickle':
            lock_file_name = os.path.splitext(blob.name)[0] + '.lock'
            if lock_file_name not in blobs:
                # scenario is available, set lock file and start
                # conflicts should be extremely rare and would result
                # in executing the same scenario twice which is acceptable
                with open(lock_file_name, 'w') as lock_file:
                    lock_file.write('{} {}'.format(
                        container_identity,
                        time.time()))
                block_blob_service.create_blob_from_path(
                    constants.CONTAINER_NAME,
                    lock_file_name,
                    os.path.abspath(lock_file_name))
                print("created lock file {}".format(lock_file_name))
                # download corresponding scenario file
                block_blob_service.get_blob_to_path(
                    constants.CONTAINER_NAME,
                    blob.name,
                    blob.name)  # save to same name locally
                print("downloaded scenario file {}".format(blob.name))
            else:
                continue
        elif os.path.splitext(blob.name)[1] == '.lock':
            block_blob_service.get_blob_to_path(
                constants.CONTAINER_NAME,
                blob.name,
                blob.name)  # save to same name locally
            
            with open(blob.name, 'r') as lock_file:
                lock_info = lock_file.read()
                if lock_info[:len(container_identity)] == container_identity:
                    # this container isn't working on the scenario, so delete lock file
                    block_blob_service.delete_blob(constants.CONTAINER_NAME, blob.name)
                    print("deleted lock file {}".format(lock_file_name))

                else:
                    # another container has locked this scenario
                    lock_expiry = float(lock_info.split(' ')[1])
                    # if lock is older than a week it's expired and can be deleted
                    if lock_expiry > time.time() + 60 * 60 * 24 * 7:
                        block_blob_service.delete_blob(constants.CONTAINER_NAME, blob.name)
                        print("deleted lock file {}".format(lock_file_name))
            continue
        else:
            raise Exception("Unexpected state - check your blob")

        print("starting execution of scenario {}".format(blob.name))
        results_file = 'results-{}'.format(blob.name.replace('.pickle', '.spickle'))
        run(config_file=blob.name, output=results_file, config_override=None)
