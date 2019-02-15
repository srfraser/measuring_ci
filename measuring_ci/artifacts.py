import asyncio
import logging
import os
from difflib import get_close_matches

import boto3
import dateutil.parser

from .utils import list_s3_objects, semaphore_wrapper

log = logging.getLogger(__name__)


def get_artifact_expiry(task_json):
    """Extract artifact expiry times from task definition.

    Uses the task's own expiry time as a default if the
    payload artifacts don't specify it.

    Copes with both list and dictionary formats in common use.
    """
    artifacts = task_json['task']['payload'].get('artifacts')
    task_expiry = task_json['task']['expires']
    # This acts as a default value
    expiries = {'': task_expiry}
    if artifacts is None:
        return expiries
    if isinstance(artifacts, list):
        expiries.update({entry['name']: entry.get('expires', task_expiry)
                         for entry in artifacts if 'name' in entry})
    elif isinstance(artifacts, dict):
        expiries.update({k: v.get('expires', task_expiry) for k, v in artifacts.items()})
    return expiries


def insert_artifact_expiry(task, s3_by_name):
    """Add expiry times to artifact metadata.

    Finds the closest match for an expiry time, as often
    the full path isn't mentioned. Closest directory if
    available, or task's own expiry time if not, using
    the default set in get_artifact_expiry.
    """
    expiries = get_artifact_expiry(task.json)
    run_ids = [r['runId'] for r in task.json['status']['runs']]
    expiries = {f"{task.taskid}/{run_id}/{k}": v for k, v in expiries.items()
                for run_id in run_ids}
    for name in s3_by_name:
        possibles = [e for e in expiries.keys() if name.startswith(e)]
        keys = get_close_matches(name, possibles, n=1, cutoff=0.3)
        key = keys[0]
        s3_by_name[name]['expires'] = dateutil.parser.parse(expiries[key])
    return s3_by_name


async def get_s3_task_artifacts(taskid,
                                bucket_name='taskcluster-public-artifacts',
                                s3_client=None):
    if s3_client is None:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('TASKCLUSTER_S3_ACCESS_KEY'),
            aws_secret_access_key=os.environ.get('TASKCLUSTER_S3_SECRET_KEY'),
        )
    prefix = taskid + '/'
    return await list_s3_objects(s3_client, bucket_name, prefix)


async def get_artifact_metadata(task):
    """Return artifact metadata for a task.

    Fetches most data from s3, and works out the rest from the
    task payload.
    """
    try:
        s3_artifacts = await get_s3_task_artifacts(task.taskid)
    except:  # noqa raises many possibilities
        return dict()
    s3_by_name = dict()
    for artifact in s3_artifacts:
        s3_by_name[artifact['Key']] = {
            'size': artifact['Size'],
            'created': artifact['LastModified'],
        }
    s3_by_name = insert_artifact_expiry(task, s3_by_name)
    return s3_by_name


async def get_artifact_costs(group):
    """Calculate artifact costs for a given task graph."""
    log.info("Fetching Taskcluster artifact info for %s", str(group))
    sem = asyncio.Semaphore(10)

    s3_tasks = []
    for t in group.tasks():
        s3_tasks.append(semaphore_wrapper(sem, get_artifact_metadata(t)))

    log.info('Gathering artifacts')
    s3_task_artifacts = await asyncio.gather(*s3_tasks)

    artifacts = {k: v for e in s3_task_artifacts for k, v in e.items()}

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
