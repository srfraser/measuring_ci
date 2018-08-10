import asyncio
import copy
import csv
import glob
import json
import logging
import re

import aiohttp
#import taskcluster
import taskcluster.async as taskcluster

# logging.basicConfig(level=logging.DEBUG)


async def main(loop):
    connector = aiohttp.TCPConnector(limit=100)
    timeout = aiohttp.ClientTimeout(total=60*60*3)
    async with aiohttp.ClientSession(loop=loop,
                                     connector=connector,
                                     timeout=timeout) as session:
        aiotasks = [[]]

        nightlies = await get_nightly_taskgraphids(session)
        with open('nightly_task_data_tmp1.csv', 'w') as csvf:
            taskwriter = csv.DictWriter(
                csvf, delimiter='\t',
                fieldnames=(
                    'kind', 'run', 'state', 'started', 'scheduled',
                    'resolved', 'date', 'build_platform',
                    'locale', 'taskid', 'decision_scheduled', 'provisioner',
                    'workertype'
                    )
                )
            taskwriter.writeheader()
            for taskid in nightlies:
                aiotasks[-1].append(
                    asyncio.ensure_future(write_data(
                        session, taskid, csvwriter=taskwriter))
                )
                if (len(aiotasks) % 45) == 0:
                    aiotasks.append([])
            while len(aiotasks):
                tasks = aiotasks.pop(0)
                await asyncio.gather(*tasks)
        # https://github.com/aio-libs/aiohttp/issues/1115
        await asyncio.sleep(0)


async def get_nightly_taskgraphids(session):
    idx = taskcluster.Index(session=session)
    queue = taskcluster.Queue(session=session)
    revision_indexes = []
    _ret = await idx.listNamespaces(
             'gecko.v2.mozilla-central.nightly')
    for year in sorted(
        [n['name']
         for n in _ret['namespaces']
         ], reverse=True
    ):
        if not year.startswith('2'):
            continue
        _ret = await idx.listNamespaces(
                 'gecko.v2.mozilla-central.nightly.{year}'.format(year=year)
                 )
        for month_ns in sorted(
            [n['namespace']
             for n in _ret['namespaces']
             ], reverse=True
        ):
            _ret = await idx.listNamespaces(month_ns)
            for day_ns in sorted(
                [n['namespace']
                 for n in _ret['namespaces']
                 ], reverse=True
            ):
                _ret = await idx.listNamespaces(
                        '{day_ns}.revision'.format(day_ns=day_ns)
                        )
                for rev_ns in sorted(
                    [n['namespace']
                     for n in _ret['namespaces']
                     ]
                ):
                    revision_indexes.append(rev_ns)
    aiotasks = []
    for rev_ns in revision_indexes:
        aiotasks.append(
            asyncio.ensure_future(
                idx.findTask(
                    '{rev_ns}.firefox.linux64-opt'.format(rev_ns=rev_ns)
                )
            )
        )
        aiotasks.append(
            asyncio.ensure_future(
                idx.findTask(
                    '{rev_ns}.mobile.android-api-16-opt'.format(rev_ns=rev_ns)
                )
            )
        )
    _ret = await asyncio.gather(*aiotasks, return_exceptions=True)
    taskids = [n['taskId'] for n in _ret if type(n) == type({})]
    aiotasks = []
    for task in taskids:
        aiotasks.append(
            asyncio.ensure_future(
                queue.task(task)
            )
        )
    _ret = await asyncio.gather(*aiotasks, return_exceptions=True)
    return [r['taskGroupId'] for r in _ret if type(r) == type({})]


async def write_data(session, taskid, csvwriter):
    queue = taskcluster.Queue(session=session)
    decision_task_status = await queue.status(taskid)
    decision_task = await queue.task(taskid)
    queue = None
    created = decision_task['created']
    scheduled = decision_task_status['status']['runs'][-1]['scheduled']
    taskjson = await get_taskgraph_json(session, taskid)
    if not taskjson:
        return
    useful_tasks = find_relevant_tasks(taskjson)
    taskjson = None
    if not useful_tasks:
        return
    print('-')
    for task in tuple(useful_tasks.keys()):
        attributes = useful_tasks.pop(task)
        print('=')
        rows = await get_task_data_rows(session, task, attributes,
                                        created,
                                        scheduled)
        for row in rows:
            csvwriter.writerow(row)
    print('%')


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
    for r in status['status'].get('runs', []):
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
    loop.run_until_complete(main(loop))
