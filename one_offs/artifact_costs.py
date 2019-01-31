#!/usr/bin/env python
import argparse
import asyncio
import logging
from functools import partial

import aiohttp
import boto3
import dateutil.parser
from taskcluster.aio import Queue

from taskhuddler.aio.graph import TaskGraph
from taskhuddler.utils import tc_options

log = logging.getLogger(__name__)


async def _semaphore_wrapper(semaphore, coro):
    async with semaphore:
        return await coro


async def get_tc_run_artifacts(taskid, runid):
    log.info('Fetching TC artifact info for %s/%s', taskid, runid)
    artifacts = []
    query = {}
    async with aiohttp.ClientSession() as session:
        queue = Queue(options=tc_options(), session=session)
        while True:
            resp = await queue.listArtifacts(taskid, runid, query=query)

            # Ammend the artifact information with the task and run ids
            # to make it easy to find the corresponding S3 object
            for a in resp['artifacts']:
                a['_name'] = f'{taskid}/{runid}/{a["name"]}'
                artifacts.append(a)
            if 'continuationToken' in resp:
                query.update({'continuationToken': resp['continuationToken']})
            else:
                break

    return artifacts


async def get_s3_task_artifacts(taskid,
                                bucket_name='taskcluster-public-artifacts',
                                s3_client=boto3.client('s3')):

    loop = asyncio.get_event_loop()
    log.info('Fetching S3 artifact info for %s', taskid)
    artifacts = []

    cont_token = None
    prefix = taskid + '/'
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


def merge_artifacts(tc_artifacts, s3_artifacts):
    tc_by_name = {a['_name']: a for a in tc_artifacts}
    s3_by_name = {a['Key']: a for a in s3_artifacts}

    retval = {}
    for name, s3_obj in s3_by_name.items():
        retval[name] = {}
        retval[name]['size'] = s3_obj['Size']
        retval[name]['expires'] = dateutil.parser.parse(tc_by_name[name]['expires'])
        retval[name]['created'] = s3_obj['LastModified']

    return retval


async def get_artifact_costs(groupid):
    log.info("Fetching taskgroup %s", groupid)
    group = await TaskGraph(groupid)

    log.info("Fetching TC artifact info")
    sem = asyncio.Semaphore(10)

    tc_tasks = []
    s3_tasks = []
    for t in group.tasks():
        for run in t.json['status']['runs']:
            runid = run['runId']
            tc_tasks.append(_semaphore_wrapper(sem, get_tc_run_artifacts(t.taskid, runid)))
        s3_tasks.append(_semaphore_wrapper(sem, get_s3_task_artifacts(t.taskid)))

    log.info('Gathering artifacts')
    tc_task_artifacts, s3_task_artifacts = await asyncio.gather(
        asyncio.gather(*tc_tasks),
        asyncio.gather(*s3_tasks),
    )

    # Flatten the lists
    s3_artifacts = [artifacts for tasks in s3_task_artifacts for artifacts in tasks]
    tc_artifacts = [artifacts for tasks in tc_task_artifacts for artifacts in tasks]
    artifacts = merge_artifacts(tc_artifacts, s3_artifacts)

    std_cost = 0.02 / (30 * 86400)  # cost per second
    std_ia_cost = 0.0125 / (30 * 86400)  # cost per second
    transition_time = 45 * 86400  # 45 days
    task_cost = 0
    task_size = 0

    for name, info in artifacts.items():
        task_size += info['size']
        gbs = info['size'] / (1024 ** 3)
        ttl_seconds = (info['expires'] - info['created']).total_seconds()
        if ttl_seconds > transition_time:
            std_seconds = transition_time
            std_ia_seconds = ttl_seconds - transition_time
        else:
            std_seconds = ttl_seconds
            std_ia_seconds = 0

        cost = ((gbs * std_seconds * std_cost) +
                (gbs * std_ia_seconds * std_ia_cost))
        task_cost += cost

    return task_size, task_cost


async def main(args):
    size, cost = await get_artifact_costs(args.groupid)
    print(f'Size: {size:,d}; cost: ${cost:,.2f}')


def parse_args():
    parser = argparse.ArgumentParser('Artifact costs')
    parser.add_argument('groupid')
    return parser.parse_args()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    loop = asyncio.get_event_loop()
    args = parse_args()
    loop.run_until_complete(main(args))
