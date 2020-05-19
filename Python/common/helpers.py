from __future__ import print_function
from datetime import datetime, timedelta
import io
import os
import time

import azure.batch as batch
from azure.batch import batch_auth, BatchServiceClient
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerSasPermissions, ResourceTypes, generate_blob_sas, generate_container_sas

def select_latest_verified_vm_image_with_node_agent_sku(batch_client, publisher, offer, sku_starts_with):
    """Select the latest verified image that Azure Batch supports given
    a publisher, offer and sku (starts with filter).

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param str publisher: vm image publisher
    :param str offer: vm image offer
    :param str sku_starts_with: vm sku starts with filter
    :rtype: tuple
    :return: (node agent sku id to use, vm image ref to use)
    """
    # get verified vm image list and node agent sku ids from service
    options = batch.models.AccountListSupportedImagesOptions(filter="verificationType eq 'verified'")
    images = batch_client.account.list_supported_images(account_list_supported_images_options=options)

    # pick the latest supported sku
    skus_to_use = [
        (image.node_agent_sku_id, image.image_reference) for image in images
        if image.image_reference.publisher.lower() == publisher.lower() and
        image.image_reference.offer.lower() == offer.lower() and
        image.image_reference.sku.startswith(sku_starts_with)
    ]

    # pick first
    agent_sku_id, image_ref_to_use = skus_to_use[0]
    return (agent_sku_id, image_ref_to_use)

def create_pool_if_not_exist(batch_client, pool):
    """Creates the specified pool if it doesn't already exist

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param pool: The pool to create.
    :type pool: `batchserviceclient.models.PoolAddParameter`
    """
    try:
        print("Attempting to create pool:", pool.id)
        batch_client.pool.add(pool)
        print("Created pool:", pool.id)
    except batch.models.BatchErrorException as e:
        if e.error.code != "PoolExists":
            raise
        else:
            print("Pool {!r} already exists".format(pool.id))

def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print('Creating job [{}]...'.format(job_id))

    job = batch.models.JobAddParameter(
        id=job_id,
        pool_info=batch.models.PoolInformation(pool_id=pool_id))

    try:
        batch_service_client.job.add(job)
    except batch.models.batch_error.BatchErrorException as err:
        print_batch_exception(err)
        if err.error.code != "JobExists":
            raise
        else:
            print("Job {!r} already exists".format(job_id))

def wait_for_all_nodes_state(batch_client, pool, node_state):
    """Waits for all nodes in pool to reach any specified state in set

    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param pool: The pool containing the node.
    :type pool: `batchserviceclient.models.CloudPool`
    :param set node_state: node states to wait for
    :rtype: list
    :return: list of `batchserviceclient.models.ComputeNode`
    """
    print('waiting for all nodes in pool {} to reach one of: {!r}'.format(
        pool.id, node_state))
    i = 0
    while True:
        # refresh pool to ensure that there is no resize error
        pool = batch_client.pool.get(pool.id)
        if pool.resize_errors is not None:
            resize_errors = "\n".join([repr(e) for e in pool.resize_errors])
            raise RuntimeError(
                'resize error encountered for pool {}:\n{}'.format(
                    pool.id, resize_errors))
        nodes = list(batch_client.compute_node.list(pool.id))
        if (len(nodes) >= pool.target_dedicated_nodes and
                all(node.state in node_state for node in nodes)):
            return nodes
        i += 1
        if i % 3 == 0:
            print('waiting for {} nodes to reach desired state...'.format(
                pool.target_dedicated_nodes))
        time.sleep(10)

def create_sas_token(blob_client, container_name, blob_name, permission, expiry=None, timeout=None):
    """Create a blob sas token

    :param blob_client: The storage blob client to use.
    :type blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the container to upload the blob to.
    :param str blob_name: The name of the blob being referenced.
    :param expiry: The SAS expiry time.
    :type expiry: `datetime.datetime`
    :param int timeout: timeout in minutes from now for expiry,
        will only be used if expiry is not specified
    :return: A SAS token
    :rtype: str
    """
    if expiry is None:
        if timeout is None:
            timeout = 30
        expiry = datetime.utcnow() + timedelta(minutes=timeout)

    return generate_blob_sas(
        blob_client.account_name,
        container_name,
        blob_name,
        account_key=blob_client.credential.account_key,
        permission=permission,
        expiry=expiry
    )

def upload_blob_and_create_sas(container_client, container_name, blob_name, file_path, expiry, timeout=None):
    """Uploads a file from local disk to Azure Storage and creates
    a SAS for it.

    :param container_client: The storage container client to use.
    :type container_client: `azure.storage.blob.ContainerClient`
    :param str container_name: The name of the container to upload the blob to.
    :param str blob_name: The name of the blob to upload the local file to.
    :param str file_name: The name of the local file to upload.
    :param expiry: The SAS expiry time.
    :type expiry: `datetime.datetime`
    :param int timeout: timeout in minutes from now for expiry,
        will only be used if expiry is not specified
    :return: A SAS URL to the blob with the specified expiry time.
    :rtype: str
    """
    with open(file_path, "rb") as data:
        blob_client = container_client.upload_blob(name=blob_name, data=data)

    sas_token = create_sas_token(
        blob_client,
        container_name,
        blob_name,
        permission=ContainerSasPermissions(read=True),
        expiry=expiry,
        timeout=timeout)

    sas_url = "https://{}.blob.core.windows.net/{}/{}?{}".format(blob_client.account_name, container_name, blob_name, sas_token)

    return sas_url

def upload_file_to_container(container_client, container_name, file_path):
    """
    Uploads a local file to an Azure Blob storage container.

    :param container_client: A container client.
    :type container_client: `azure.storage.blob.ContainerClient`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    blob_name = os.path.basename(file_path)
    print('Uploading file {} to container [{}]...'.format(file_path, container_name))
    sas_url = upload_blob_and_create_sas(container_client, container_name, blob_name, file_path, expiry=datetime.utcnow() + timedelta(hours=2))
    return batch.models.ResourceFile(file_path=blob_name, http_url=sas_url)


def download_blob_from_container(blob_client, container_name, blob_name, directory_path):
    """
    Downloads specified blob from the specified Azure Blob storage container.

    :param blob_client: A blob service client.
    :type blob_client: `azure.storage.blob.BlockBlobService`
    :param container_name: The Azure Blob storage container from which to
        download file.
    :param blob_name: The name of blob to be downloaded
    :param directory_path: The local directory to which to download the file.
    """
    print('Downloading result file from container [{}]...'.format(container_name))

    destination_file_path = os.path.join(directory_path, blob_name)

    with open(destination_file_path, "wb") as my_blob:
        blob_data = blob_client.get_blob_client(container_name, blob_name).download_blob()
        blob_data.readinto(my_blob)

    print('  Downloaded blob [{}] from container [{}] to {}'.format(blob_name, container_name, destination_file_path))
    print('  Download complete!')

def generate_unique_resource_name(resource_prefix):
    """Generates a unique resource name by appending a time
    string after the specified prefix.

    :param str resource_prefix: The resource prefix to use.
    :return: A string with the format "resource_prefix-<time>".
    :rtype: str
    """
    return resource_prefix + "-" + datetime.utcnow().strftime("%Y%m%d-%H%M%S")

def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if (batch_exception.error and batch_exception.error.message and
            batch_exception.error.message.value):
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for msg in batch_exception.error.values:
                print('{}:\t{}'.format(msg.key, msg.value))
    print('-------------------------------------------')

def wrap_commands_in_shell(ostype, commands):
    """Wrap commands in a shell

    :param list commands: list of commands to wrap
    :param str ostype: OS type, linux or windows
    :rtype: str
    :return: a shell wrapping commands
    """
    if ostype.lower() == 'linux':
        return '/bin/bash -c \'set -e; set -o pipefail; {}; wait\''.format(
            ';'.join(commands))
    elif ostype.lower() == 'windows':
        return 'cmd.exe /c "{}"'.format('&'.join(commands))
    else:
        raise ValueError('unknown ostype: {}'.format(ostype))