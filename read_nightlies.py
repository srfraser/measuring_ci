import asyncio
import copy
import csv
import glob
import json
import logging
import re

import aiohttp
import aiodns
#import taskcluster
import taskcluster.aio as taskcluster
# from measuring_ci.nightly import get_nightly_taskgraphids

logging.basicConfig(level=logging.INFO)

log = logging.getLogger()


async def main():

    fieldnames = (
        'kind', 'run', 'state', 'started',
        'scheduled', 'resolved', 'date', 'build_platform',
        'locale', 'taskid', 'decision_scheduled', 'provisioner',
        'workertype'
    )

    loop = asyncio.get_event_loop()

    connector = aiohttp.TCPConnector(limit=100,
                                     resolver=aiohttp.resolver.AsyncResolver())
    timeout = aiohttp.ClientTimeout(total=60*60*3)
    async with aiohttp.ClientSession(loop=loop,
                                     connector=connector,
                                     timeout=timeout) as session:

        log.info("Searching for nightly task graphs")
        nightlies = await get_nightly_taskgraphids(session)
        log.info("Found %d nightly task graphs", len(nightlies))

        with open('nightly_task_data_tmp1.csv', 'w') as csvf:
            semaphore = asyncio.Semaphore(5)

            taskwriter = csv.DictWriter(
                csvf, delimiter='\t',
                fieldnames=fieldnames
            )
            taskwriter.writeheader()

            aiotasks = list()
            for taskid in nightlies:
                aiotasks.append(
                    asyncio.ensure_future(write_data(
                        session, taskid, csvwriter=taskwriter, semaphore=semaphore))
                )

            await asyncio.gather(*aiotasks)
        # https://github.com/aio-libs/aiohttp/issues/1115
        await asyncio.sleep(0)


async def _semaphore_wrapper(action, arg, semaphore):
    async with semaphore:
        return await action(arg)


async def get_nightly_taskgraphids(session):
    idx = taskcluster.Index(session=session)
    queue = taskcluster.Queue(session=session)
    revision_indexes = []
    _ret = await idx.listNamespaces('gecko.v2.mozilla-central.nightly')

    namespaces = sorted(
        [n['namespace'] for n in _ret['namespaces'] if n['name'].startswith('2')],
        reverse=True
    )

    # Recurse down through the namespaces until we have reached 'revision'
    for _ in ['years', 'months', 'days', 'revision']:
        tasks = [asyncio.ensure_future(idx.listNamespaces(n)) for n in namespaces]
        ret = await asyncio.gather(*tasks)
        namespaces = sorted(n['namespace'] for m in ret for n in m['namespaces'])

    # Remove 'latest'
    revision_indexes = [n for n in namespaces if 'revision' in n]

    semaphore = asyncio.Semaphore(50)

    aiotasks = []
    for rev_ns in revision_indexes:
        aiotasks.append(
            asyncio.ensure_future(
                _semaphore_wrapper(
                    idx.findTask,
                    '{rev_ns}.firefox.linux64-opt'.format(rev_ns=rev_ns),
                    semaphore=semaphore
                )
            )
        )
        aiotasks.append(
            asyncio.ensure_future(
                _semaphore_wrapper(
                    idx.findTask,
                    '{rev_ns}.mobile.android-api-16-opt'.format(rev_ns=rev_ns),
                    semaphore=semaphore
                )
            )
        )
    _ret = await asyncio.gather(*aiotasks, return_exceptions=True)
    taskids = [n['taskId'] for n in _ret if isinstance(n, dict)]

    aiotasks = []
    for task in taskids:
        aiotasks.append(
            asyncio.ensure_future(
                _semaphore_wrapper(
                    queue.task,
                    task,
                    semaphore=semaphore
                )
            )
        )
    _ret = await asyncio.gather(*aiotasks, return_exceptions=True)
    return [r['taskGroupId'] for r in _ret if isinstance(r, dict)]


async def write_data(session, taskid, csvwriter, semaphore):
    queue = taskcluster.Queue(session=session)
    decision_task_status = await queue.status(taskid)
    decision_task = await queue.task(taskid)
    queue = None
    created = decision_task['created']
    scheduled = decision_task_status['status']['runs'][-1]['scheduled']

    async with semaphore:
        log.info("Downloading task json object for %s", taskid)
        taskjson = await get_taskgraph_json(session, taskid)
        if not taskjson:
            log.info("task json was empty")
            return
        log.info("task json for %s had %d entries", taskid, len(taskjson))
        useful_tasks = find_relevant_tasks(taskjson)
        taskjson = None
        if not useful_tasks:
            return
        log.info("Processing rows for %s", taskid)
        for task in tuple(useful_tasks.keys()):
            attributes = useful_tasks.pop(task)

            rows = await get_task_data_rows(session, task, attributes,
                                            created,
                                            scheduled)
            for row in rows:
                csvwriter.writerow(row)
    log.info("Wrote data for %s", taskid)


async def get_task_data_rows(session, taskid, attributes, created,
                             scheduled):
    queue = taskcluster.Queue(session=session)
    try:
        status = await queue.status(taskid)
    except taskcluster.exceptions.TaskclusterRestFailure:
        return []
    except:
        print("CALLEK-SOMETHING WENT WRONG")
        raise
    rows = []
    status = status['status']

    for r in status.get('runs', []):
        row = {'kind': attributes.get('kind')}
        row['provisioner'] = status['provisionerId']
        row['workertype'] = status['workerType']
        row['taskid'] = taskid
        row['run'] = r['runId']
        row['state'] = r['state']
        row['started'] = r.get('started')
        row['scheduled'] = r['scheduled']
        row['resolved'] = r['resolved']
        row['date'] = created
        row['decision_scheduled'] = scheduled
        row['build_platform'] = attributes.get('build_platform')
        if 'locale' in attributes:
            row['locale'] = attributes['locale']
            rows.append(row)
        elif 'chunk_locales' in attributes:
            for l in attributes['chunk_locales']:
                row_ = copy.deepcopy(row)
                row_['locale'] = l
                rows.append(row_)
        elif 'all_locales' in attributes:
            for l in attributes['all_locales']:
                row_ = copy.deepcopy(row)
                row_['locale'] = l
                rows.append(row_)
    return rows


async def get_taskgraph_json(session, taskid):
    queue = taskcluster.Queue(session=session)
    artifact = {}
    try:
        artifact = await queue.getLatestArtifact(taskid, 'public/task-graph.json')
    except taskcluster.exceptions.TaskclusterRestFailure as e:
        pass
    return artifact


def find_relevant_tasks(task_graph_json):
    wanted_attributes = {'locale', 'chunk_locales', 'all_locales'}
    return {task: definition["attributes"]
            for task, definition in task_graph_json.items()
            if set(definition["attributes"].keys()) & wanted_attributes}


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
