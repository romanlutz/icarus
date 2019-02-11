from azure.common.credentials import ServicePrincipalCredentials
from azureml.core.authentication import ServicePrincipalAuthentication


def get_sp_auth(name, password, tenant):
    return ServicePrincipalAuthentication(
        tenant_id=tenant,
        username=name,
        password=password
    )

def get_sp_credentials(name, password, tenant):
    return ServicePrincipalCredentials(
        client_id=name,
        secret=password,
        tenant=tenant
    )