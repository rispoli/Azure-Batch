from __future__ import print_function
from datetime import datetime, timedelta
import os
import sys
import time

import azure.batch as batch
from azure.batch import batch_auth, BatchServiceClient
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerSasPermissions, ResourceTypes, generate_blob_sas, generate_container_sas

def generate_unique_resource_name(resource_prefix):
    """Generates a unique resource name by appending a time
    string after the specified prefix.

    :param str resource_prefix: The resource prefix to use.
    :return: A string with the format "resource_prefix-<time>".
    :rtype: str
    """
    return resource_prefix + "-" + datetime.utcnow().strftime("%Y%m%d-%H%M%S")

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

    sas_url = "https://{}.blob.core.windows.net/{}/{}?{}".format(_STORAGE_ACCOUNT_NAME, container_name, blob_name, sas_token)

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

def create_pool_and_wait_for_vms(
        batch_service_client, pool_id,
        publisher, offer, sku, vm_size,
        target_dedicated_nodes,
        command_line=None, resource_files=None,
        elevation_level=batch.models.ElevationLevel.admin):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace Image publisher
    :param str offer: Marketplace Image offer
    :param str sku: Marketplace Image sku
    :param str vm_size: The size of VM, eg 'Standard_A1' or 'Standard_D1' per
    https://azure.microsoft.com/en-us/documentation/articles/
    virtual-machines-windows-sizes/
    :param int target_dedicated_nodes: Number of target VMs for the pool
    :param str command_line: command line for the pool's start task.
    :param list resource_files: A collection of resource files for the pool's
    start task.
    :param str elevation_level: Elevation level the task will be run as;
        either 'admin' or 'nonadmin'.
    """
    print('Creating pool [{}]...'.format(pool_id))

    # If no start task is specified enable Batch Insights (https://github.com/Azure/batch-insights)
    if not command_line:
        command_line = "/bin/bash -c 'wget  -O - https://raw.githubusercontent.com/Azure/batch-insights/master/centos.sh | bash'"

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/

    # Get the virtual machine configuration for the desired distro and version.
    # For more information about the virtual machine configuration, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
    sku_to_use, image_ref_to_use = select_latest_verified_vm_image_with_node_agent_sku(batch_service_client, publisher, offer, sku)
    user = batch.models.AutoUserSpecification(
        scope=batch.models.AutoUserScope.pool,
        elevation_level=elevation_level)
    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batch.models.VirtualMachineConfiguration(
            image_reference=image_ref_to_use,
            node_agent_sku_id=sku_to_use),
        vm_size=vm_size,
        target_dedicated_nodes=target_dedicated_nodes,
        resize_timeout=timedelta(minutes=15),
        enable_inter_node_communication=True,
        max_tasks_per_node=1,
        start_task=batch.models.StartTask(
            command_line=command_line,
            user_identity=batch.models.UserIdentity(auto_user=user),
            wait_for_success=True,
            resource_files=resource_files) if command_line else None,
    )

    create_pool_if_not_exist(batch_service_client, new_pool)

    # because we want all nodes to be available before any tasks are assigned
    # to the pool, here we will wait for all compute nodes to reach idle
    nodes = wait_for_all_nodes_state(
        batch_service_client, new_pool,
        frozenset(
            (batch.models.ComputeNodeState.start_task_failed,
             batch.models.ComputeNodeState.unusable,
             batch.models.ComputeNodeState.idle)
        )
    )
    # ensure all node are idle
    if any(node.state != batch.models.ComputeNodeState.idle for node in nodes):
        raise RuntimeError('node(s) of pool {} not in idle state'.format(
            pool_id))

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

def add_task(
        batch_service_client, job_id, task_id, num_instances,
        application_cmdline, input_files, elevation_level,
        output_file_names, output_container_sas,
        coordination_cmdline, common_files):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID of the job to which to add the task.
    :param str task_id: The ID of the task to be added.
    :param str application_cmdline: The application commandline for the task.
    :param list input_files: A collection of input files.
    :param elevation_level: Elevation level used to run the task; either
     'admin' or 'nonadmin'.
    :type elevation_level: `azure.batch.models.ElevationLevel`
    :param int num_instances: Number of instances for the task
    :param str coordination_cmdline: The application commandline for the task.
    :param list common_files: A collection of common input files.
    """

    print('Adding {} task to job [{}]...'.format(task_id, job_id))

    multi_instance_settings = None
    if coordination_cmdline or (num_instances and num_instances > 1):
        multi_instance_settings = batch.models.MultiInstanceSettings(
            number_of_instances=num_instances,
            coordination_command_line=coordination_cmdline,
            common_resource_files=common_files)
    user = batch.models.AutoUserSpecification(
        scope=batch.models.AutoUserScope.pool,
        elevation_level=elevation_level)
    output_file = batch.models.OutputFile(
        file_pattern=output_file_names,
        destination=batch.models.OutputFileDestination(
            container=batch.models.OutputFileBlobContainerDestination(
                container_url=output_container_sas)),
        upload_options=batch.models.OutputFileUploadOptions(
            upload_condition=batch.models.
            OutputFileUploadCondition.task_completion))
    task = batch.models.TaskAddParameter(
        id=task_id,
        command_line=application_cmdline,
        user_identity=batch.models.UserIdentity(auto_user=user),
        resource_files=input_files,
        multi_instance_settings=multi_instance_settings,
        output_files=[output_file])
    batch_service_client.task.add(job_id, task)

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

def wait_for_subtasks_to_complete(
        batch_service_client, job_id, task_id, timeout):
    """
    Returns when all subtasks in the specified task reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be to monitored.
    :param str task_id: The id of the task whose subtasks should be monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.now() + timeout

    print("Monitoring all tasks for 'Completed' state, timeout in {}..."
          .format(timeout), end='')

    while datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()

        subtasks = batch_service_client.task.list_subtasks(job_id, task_id)
        incomplete_subtasks = [subtask for subtask in subtasks.value if
                               subtask.state !=
                               batch.models.TaskState.completed]

        if not incomplete_subtasks:
            print()
            return True
        else:
            time.sleep(10)

    print()
    raise RuntimeError(
        "ERROR: Subtasks did not reach 'Completed' state within "
        "timeout period of " + str(timeout))

def wait_for_tasks_to_complete(batch_service_client, job_id, timeout):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be to monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.now() + timeout

    print("Monitoring all tasks for 'Completed' state, timeout in {}..."
          .format(timeout), end='')

    while datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        for task in tasks:
            if task.state == batch.models.TaskState.completed:
                # Pause execution until subtasks reach Completed state.
                wait_for_subtasks_to_complete(batch_service_client, job_id,
                                              task.id,
                                              datetime.timedelta(minutes=10))

        incomplete_tasks = [task for task in tasks if
                            task.state != batch.models.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True
        else:
            time.sleep(10)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))

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

_BATCH_ACCOUNT_KEY = '***REMOVED***'
_BATCH_ACCOUNT_NAME = '***REMOVED***'
_BATCH_ACCOUNT_URL = 'https://' + _BATCH_ACCOUNT_NAME + '.westeurope.batch.azure.com'

_STORAGE_ACCOUNT_KEY = '***REMOVED***'
_STORAGE_ACCOUNT_NAME = '***REMOVED***'
_STORAGE_ACCOUNT_URL = 'https://' + _STORAGE_ACCOUNT_NAME + '.blob.core.windows.net/'

_OS_NAME = 'linux'
_APP_NAME = 'pingpong'
_POOL_ID = generate_unique_resource_name('pool_{}_{}'.format(_OS_NAME, _APP_NAME))
_POOL_NODE_COUNT = 3
_POOL_VM_SIZE = 'Standard_H16r'
_NODE_OS_PUBLISHER = 'OpenLogic'
_NODE_OS_OFFER = 'CentOS-HPC'
_NODE_OS_SKU = '7.4'
_JOB_ID = 'job-{}'.format(_POOL_ID)
_TASK_ID = generate_unique_resource_name('task_{}_{}'.format(_OS_NAME, _APP_NAME))
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
        upload_file_to_container(container_client, input_container_name, file_path)
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
        upload_file_to_container(container_client, input_container_name, file_path)
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
    create_pool_and_wait_for_vms(batch_client, _POOL_ID, _NODE_OS_PUBLISHER, _NODE_OS_OFFER, _NODE_OS_SKU, _POOL_VM_SIZE, _POOL_NODE_COUNT)

    # Create the job that will run the tasks.
    create_job(batch_client, _JOB_ID, _POOL_ID)

    # Add the tasks to the job.  We need to supply a container shared access
    # signature (SAS) token for the tasks so that they can upload their output
    # to Azure Storage.
    add_task(
        batch_client, _JOB_ID, _TASK_ID, _NUM_INSTANCES,
        wrap_commands_in_shell(_OS_NAME, application_cmdline),
        input_files, batch.models.ElevationLevel.non_admin,
        _TASK_OUTPUT_FILE_PATH_ON_VM, output_container_sas,
        wrap_commands_in_shell(_OS_NAME, coordination_cmdline),
        common_files)

    # Pause execution until task (and all subtasks for a multiinstance task)
    # reach Completed state.
    wait_for_tasks_to_complete(batch_client, _JOB_ID, timedelta(minutes=120))

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
    download_blob_from_container(blob_service_client, output_container_name, _TASK_OUTPUT_BLOB_NAME, os.path.expanduser('/tmp'))
    download_blob_from_container(blob_service_client, output_container_name, _TASK_ERROR_BLOB_NAME, os.path.expanduser('/tmp'))

    # Clean up storage resources
    for blob in container_client.list_blobs():
        container_client.delete_blob(blob)

    # Clean up Batch resources (if the user so chooses).
    batch_client.job.delete(_JOB_ID)
    batch_client.pool.delete(_POOL_ID)