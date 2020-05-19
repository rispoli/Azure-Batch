from __future__ import print_function
from datetime import datetime, timedelta
import os
import sys
import time

import azure.batch as batch
from azure.batch import batch_auth, BatchServiceClient
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerSasPermissions, ResourceTypes, generate_blob_sas, generate_container_sas

import multi_task_helpers

sys.path.append('.')
import common.helpers  # noqa

_BATCH_ACCOUNT_KEY = 'oSznfJa8SaOzugl0HKPjW8jZGzdBSWYy+/iZ130lrhi5i1ZL9DZENq34a8RdFcx+/V17JZjRvHyC6bOh+3JfKQ=='
_BATCH_ACCOUNT_NAME = 'dr2020batchtests'
_BATCH_ACCOUNT_URL = 'https://' + _BATCH_ACCOUNT_NAME + '.westeurope.batch.azure.com'

_STORAGE_ACCOUNT_KEY = 'Q9JjZlDu9kS9E8PKlKtQ0vItozEZQ2zKmfDXfdBfKw02F3QrmoY74OnRXTHMJtPJ1YHgUQhtBAhrKEA858N5LA=='
_STORAGE_ACCOUNT_NAME = 'rltdatastagingdr'
_STORAGE_ACCOUNT_URL = 'https://' + _STORAGE_ACCOUNT_NAME + '.blob.core.windows.net/'

_OS_NAME = 'linux'
_APP_NAME = 'pingpong'
_POOL_ID = common.helpers.generate_unique_resource_name('pool_{}_{}'.format(_OS_NAME, _APP_NAME))
_POOL_NODE_COUNT = 3
_POOL_VM_SIZE = 'Standard_H16r'
_NODE_OS_PUBLISHER = 'OpenLogic'
_NODE_OS_OFFER = 'CentOS-HPC'
_NODE_OS_SKU = '7.4'
_JOB_ID = 'job-{}'.format(_POOL_ID)
_TASK_ID = common.helpers.generate_unique_resource_name('task_{}_{}'.format(_OS_NAME, _APP_NAME))
_TASK_OUTPUT_FILE_PATH_ON_VM = '../std*.txt'
_TASK_OUTPUT_BLOB_NAME = 'stdout.txt'
_TASK_ERROR_BLOB_NAME = 'stderr.txt'
_NUM_INSTANCES = _POOL_NODE_COUNT

if __name__ == '__main__':
    start_time = datetime.now().replace(microsecond=0)
    print('Sample start: {}'.format(start_time))
    print()

    # Instantiate a BlobServiceClient using a connection string
    blob_service_client = BlobServiceClient(account_url=_STORAGE_ACCOUNT_URL, credential=_STORAGE_ACCOUNT_KEY)

    # The containers in Azure Storage.
    input_container_name = 'dsi'
    output_container_name = 'dso'

    # Obtain a shared access signature that provides write access to the output
    # container to which the tasks will upload their output.
    output_container_sas = generate_container_sas(
        blob_service_client.account_name,
        output_container_name,
        account_key=blob_service_client.credential.account_key,
        permission=ContainerSasPermissions(list=True, read=True, write=True),
        expiry=datetime.utcnow() + timedelta(hours=2)
        )

    output_container_sas = 'https://{}.blob.core.windows.net/{}?{}'.format(_STORAGE_ACCOUNT_NAME, output_container_name, output_container_sas)

    # The collection of common scripts/data files that are to be
    # used/processed by all subtasks (including primary) in a
    # multi-instance task.
    common_file_paths = [os.path.realpath('./data/coordination-cmd')]

    # Instantiate a ContainerClient
    container_client = blob_service_client.get_container_client(input_container_name)

    # Upload the common script/data files to Azure Storage
    common_files = [
        common.helpers.upload_file_to_container(container_client, input_container_name, file_path)
        for file_path in common_file_paths]

    # Command to run on all subtasks including primary before starting
    # application command on primary.
    coordination_cmdline = [
        '$AZ_BATCH_TASK_SHARED_DIR/coordination-cmd']

    # The collection of scripts/data files that are to be used/processed by
    # the task (used/processed by primary in a multiinstance task).
    input_file_paths = [
        os.path.realpath('./data/application-cmd')]

    # Upload the script/data files to Azure Storage
    input_files = [
        common.helpers.upload_file_to_container(container_client, input_container_name, file_path)
        for file_path in input_file_paths]

    # Main application command to execute multiinstance task on a group of
    # nodes, eg. MPI.
    application_cmdline = ['$AZ_BATCH_TASK_WORKING_DIR/application-cmd {}'.format(_NUM_INSTANCES)]

    # Create a Batch service client.  We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batch_auth.SharedKeyCredentials(_BATCH_ACCOUNT_NAME, _BATCH_ACCOUNT_KEY)
    batch_client = BatchServiceClient(credentials, batch_url=_BATCH_ACCOUNT_URL)

    # Create the pool that will contain the compute nodes that will execute the
    # tasks. The resource files we pass in are used for configuring the pool's
    # start task, which is executed each time a node first joins the pool (or
    # is rebooted or re-imaged).
    multi_task_helpers.create_pool_and_wait_for_vms(batch_client, _POOL_ID, _NODE_OS_PUBLISHER, _NODE_OS_OFFER, _NODE_OS_SKU, _POOL_VM_SIZE, _POOL_NODE_COUNT)

    # Create the job that will run the tasks.
    common.helpers.create_job(batch_client, _JOB_ID, _POOL_ID)

    # Add the tasks to the job.  We need to supply a container shared access
    # signature (SAS) token for the tasks so that they can upload their output
    # to Azure Storage.
    multi_task_helpers.add_task(
        batch_client, _JOB_ID, _TASK_ID, _NUM_INSTANCES,
        common.helpers.wrap_commands_in_shell(_OS_NAME, application_cmdline),
        input_files, batch.models.ElevationLevel.non_admin,
        _TASK_OUTPUT_FILE_PATH_ON_VM, output_container_sas,
        common.helpers.wrap_commands_in_shell(_OS_NAME, coordination_cmdline),
        common_files)

    # Pause execution until task (and all subtasks for a multiinstance task)
    # reach Completed state.
    multi_task_helpers.wait_for_tasks_to_complete(batch_client, _JOB_ID, timedelta(minutes=120))

    print("Success! Task reached the 'Completed' state within the specified timeout period.")

    # Print out some timing info
    end_time = datetime.now().replace(microsecond=0)
    print()
    print('Sample end: {}'.format(end_time))
    print('Elapsed time: {}'.format(end_time - start_time))
    print()

    raw_input('Press ENTER to trigger clean-up and exit...')

    # Download the task output files from the output Storage container to a
    # local directory
    common.helpers.download_blob_from_container(blob_service_client, output_container_name, _TASK_OUTPUT_BLOB_NAME, os.path.expanduser('/tmp'))
    common.helpers.download_blob_from_container(blob_service_client, output_container_name, _TASK_ERROR_BLOB_NAME, os.path.expanduser('/tmp'))

    # Clean up storage resources
    for blob in container_client.list_blobs():
        container_client.delete_blob(blob)

    # Clean up Batch resources (if the user so chooses).
    batch_client.job.delete(_JOB_ID)
    batch_client.pool.delete(_POOL_ID)