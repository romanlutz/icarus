import os

SUBSCRIPTION_ID = '49e80366-b27c-4b2e-b59d-89c5554ec8f0'
RESOURCE_GROUP_NAME = 'cache-simulation'
WORKSPACE_NAME = 'cachesimulation'
STORAGE_ACCOUNT_NAME = 'cachetraces'
CONTAINER_NAME = 'experiments'

SP_TENANT = os.getenv('SP_TENANT')
SP_NAME = os.getenv('SP_NAME')
SP_PASSWORD = os.getenv('SP_PASSWORD')

