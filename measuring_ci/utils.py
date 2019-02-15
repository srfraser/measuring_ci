import asyncio
import os
from functools import partial
from urllib.parse import urlparse

import boto3


def tc_options():
    """Set Taskcluster options."""
    return {
        'rootUrl': os.environ.get('TASKCLUSTER_ROOT_URL', 'https://taskcluster.net'),
    }


async def semaphore_wrapper(semaphore, coro):
    async with semaphore:
        return await coro


async def list_s3_objects(s3_client, bucket_name, prefix):
    """Handle the list_objects_v2 calls."""
    loop = asyncio.get_event_loop()
    artifacts = []
    cont_token = None
    while True:
        if cont_token:
            kwargs = dict(Bucket=bucket_name, Prefix=prefix,
                          ContinuationToken=cont_token)
        else:
            kwargs = dict(Bucket=bucket_name, Prefix=prefix)

        func = partial(s3_client.list_objects_v2, **kwargs)
        resp = await loop.run_in_executor(None, func)
        if resp['KeyCount'] == 0:
            break
        artifacts.extend(resp['Contents'])
        if not resp['IsTruncated']:
            break
        cont_token = resp['NextContinuationToken']
    return artifacts


async def find_staged_data_files(s3_url):
    """Find the single-entry data files in s3."""
    url_obj = urlparse(s3_url)
    bucket_name = url_obj.netloc
    prefix = url_obj.path.lstrip('/')
    if not prefix.endswith('/'):
        prefix = prefix + '/'
    s3_client = boto3.client('s3')

    return [a['Key'] for a in await list_s3_objects(s3_client, bucket_name, prefix)]
