#!/usr/bin/env python

import boto3
import click
from time import sleep


def check_cluster_state(cluster_id, state):
    """
    Check the status of a cluster.

    Check the status of a cluster identified by cluster_id against provided string
    and returns True if it matches and False otherwise.

    Args:
        cluster_id  (str): CloudHSM clusterId, as string.
        state       (str): State to compare, as string.

    Returns:
        True if the state matches the passed string, False otherwise.
    """

    client = boto3.client('cloudhsmv2')
    response = client.describe_clusters(Filters={'clusterIds': [cluster_id]})

    if response['Clusters'][0]['State'] == state:
        return True
    else:
        return False


def check_hsm_state(cluster_id, hsm_id, state):
    """
    Check the status of an HSM instance.

    Check the status of an HSM instance identified by cluster_id and hsm_id against
    provided string and returns True if it matches and False otherwise.

    Args:
        cluster_id  (str): CloudHSM clusterId, as string.
        hsm_id      (str): HSM instance HsmId, as string.
        state       (str): State to compare, as string.

    Returns:
        True if the state matches the passed string, False otherwise.
    """

    client = boto3.client('cloudhsmv2')
    response = client.describe_clusters(Filters={'clusterIds': [cluster_id]})

    for hsm in response['Clusters'][0]['Hsms']:
        if hsm['HsmId'] == hsm_id:
            if hsm['State'] == state:
                return True
            else:
                return False


def set_cluster_hsm_count(cluster_id, cluster_hsm_count):
    """
    Create or delete cluster HSM instances.

    Checks the number of current HSM instances in a cluster identified by cluster_id
    and depending on the amount required provided in cluster_hsm_count creates or deletes
    HSM instances.

    Args:
        cluster_id          (str): CloudHSM clusterId, as string.
        cluster_hsm_count   (int): Required number of HSM instances in the cluster, as integer.

    Returns:
        None.
    """

    client = boto3.client('cloudhsmv2')
    response = client.describe_clusters(Filters={'clusterIds': [cluster_id]})
    current_hsm_count = len(response['Clusters'][0]['Hsms'])

    if current_hsm_count == cluster_hsm_count:
        print('* No new HSM instances are required in cluster: {}.'.format(
            cluster_id))

    elif current_hsm_count == 1 and check_cluster_state(cluster_id, 'UNINITIALIZED'):
        print('** Number of HSM instances in cluster: {} is lower than required but the cluster must be '
              'initialised first to increase the number of HSM instances.'.format(cluster_id))

    elif current_hsm_count < cluster_hsm_count:
        print(
            '** Number of HSM instances in cluster: {} is lower than required.'.
            format(cluster_id))
        for i in xrange(cluster_hsm_count):
            response = client.create_hsm(
                ClusterId=cluster_id, AvailabilityZone=cluster_az)
            hsm_id = response['Hsm']['HsmId']
            print('** Creating HSM instance: {} in cluster: {}.'.format(
                hsm_id, cluster_id))

            limit_counter = 0
            while not check_hsm_state(cluster_id, hsm_id, 'ACTIVE') and limit_counter < 15:
                print(
                    '* Waiting for initialization of HSM instance: {}, sleeping for 60 seconds.'.
                    format(hsm_id))
                limit_counter += 1
                sleep(60)
            print('* HSM instance: {} is ready.'.format(hsm_id))

    elif current_hsm_count > cluster_hsm_count:
        print(
            '** Number of HSM instances in cluster: {} is higher than required.'.
            format(cluster_id))

        for i in xrange(current_hsm_count - cluster_hsm_count):
            hsm_id = response['Clusters'][0]['Hsms'][i]['HsmId']
            client.delete_hsm(ClusterId=cluster_id, HsmId=hsm_id)
            print('** Deleting HSM instance: {} in cluster: {}.'.format(
                hsm_id, cluster_id))


def set_cluster_tags(cluster_id, cluster_tag_key, cluster_tag_value):
    """
    Set Cloud HSM cluster tags.

    Sets predefined tag key/value pair on cluster identified by cluster_id in order to be able to simulate
    idemnpotency by detecting clusters to process by their tags.

    Args:
        cluster_id          (str): CloudHSM clusterId, as string.
        cluster_tag_key     (str): Cluster tag name, as string.
        cluster_tag_value   (str): Cluster tag value, as string.

    Returns:
        None.
    """

    client = boto3.client('cloudhsmv2')
    client.tag_resource(
        ResourceId=cluster_id,
        TagList=[{
            'Key': cluster_tag_key,
            'Value': cluster_tag_value
        }])
    print('** Tagged cluster: {} with tag name: {} and value: {}.'.format(
        cluster_id, cluster_tag_key, cluster_tag_value))


def init_cluster(cluster_subnet_id, hsm_type='hsm1.medium'):
    """
    Initialise Cloud HSM cluster.

    Initialise Cloud HSM cluster in a subnet identified by cluster_subnet_idc with HSM instance type of hsm_type,
    currently defaulting to hsm1.medium.

    Args:
        cluster_subnet_id   (str): Subnet in which CloudHSM cluster should be located, must be within cluster az.
        hsm_type            (str): HSM instance type, currently only hsm1.medium is avalable.

    Returns:
        None.
    """

    client = boto3.client('cloudhsmv2')
    response = client.create_cluster(
        SubnetIds=[cluster_subnet_id], HsmType=hsm_type)
    cluster_id = response['Cluster']['ClusterId']
    print('** Created cluster: {}.'.format(cluster_id))

    limit_counter = 0
    while not check_cluster_state(cluster_id, 'UNINITIALIZED') and limit_counter < 20:
        print(
            '* Waiting for creation of cluster: {}, sleeping for 10 seconds.'.
            format(cluster_id))
        limit_counter += 1
        sleep(10)
    print('* Cluster: {} is ready.'.format(cluster_id))


@click.command()
@click.argument('cluster_tag_key')
@click.argument('cluster_tag_value')
@click.argument('cluster_subnet_id')
@click.argument('cluster_az')
@click.argument('cluster_hsm_count')
def create_cluster(cluster_tag_key, cluster_tag_value, cluster_subnet_id, cluster_az, cluster_hsm_count):
    """
    Create or update Cloud HSM cluster.

    Create or update Cloud HSM cluster identified by cluster_tag_key and cluster_tag_value in order
    to deliver emulated resource idempotency.

    Args:
        cluster_tag_key     (str): Tag key name used to identify the cluster to work on.
        cluster_tag_value   (str): Tag value used to identify the cluster to work on.
        cluster_subnet_id   (str): Subnet in which CloudHSM cluster should be located, must be within cluster az.
        cluster_az          (str): Availability Zone in which cluster should be located, must contain cluster subnet.
        cluster_hsm_count   (int): Number of HSM instances required in the cluster.

    Returns:
        None.
    """

    cluster_id = None
    client = boto3.client('cloudhsmv2')

    # iterate over clusters list to detect if required cluster exists
    print('* Checking clusters list for cluster tag name: {} and value: {}.'.
          format(cluster_tag_key, cluster_tag_value))
    response = client.describe_clusters()

    for cluster in response['Clusters']:
        response = client.list_tags(ResourceId=cluster['ClusterId'])

        for tag in response['TagList']:
            if tag['Key'] == cluster_tag_key and tag['Value'] == cluster_tag_value:
                cluster_id = cluster['ClusterId']
                break

        # if the cluster_id is None at this stage, a cluster with provided tags value
        # was found and there is no need to continue searching
        if cluster_id is not None:
            print('* Found cluster: {}.'.format(cluster_id))
            break

    # create required cluster if it wasn't found
    if cluster_id is None:
        print('** Required cluster not found, creating new one.')
        init_cluster(cluster_subnet_id)

        # tag new cluster with required name and value so that resource would be artificially idemnpotent
        set_cluster_tags(cluster_id, cluster_tag_key, cluster_tag_value)

    # check number of hsm's and create or delete as needed
    set_cluster_hsm_count(cluster_id, cluster_hsm_count)


if __name__ == "__main__":
    create_cluster()
