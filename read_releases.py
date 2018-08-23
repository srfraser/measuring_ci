import asyncio
import copy
import csv
import glob
import json
import logging
import re

import aiohttp
#import taskcluster
import taskcluster.aio as taskcluster

import pandas

#logging.basicConfig(level=logging.DEBUG)


async def main(loop):
    connector = aiohttp.TCPConnector(limit=100)
    timeout = aiohttp.ClientTimeout(total=60*60*3)
    async with aiohttp.ClientSession(loop=loop,
                                     connector=connector,
                                     timeout=timeout) as session:
        aiotasks = []
        with open('config.json') as f:
            config = json.load(f)

        release_graphs = read_release_taskgraph_ids(config['rw_data_path'])
        with open('task_data_temporary.csv', 'w') as csvf:
            taskwriter = csv.DictWriter(
                csvf, delimiter='\t',
                fieldnames=(
                    'kind', 'run', 'state', 'started', 'scheduled',
                    'resolved', 'product', 'version', 'build_platform',
                    'locale', 'taskid', 'decision_scheduled',
                    'display_version', 'provisioner', 'workertype'
                    )
                )
            taskwriter.writeheader()
            for taskid, graph_data in release_graphs.items():
                aiotasks.append(
                    asyncio.ensure_future(write_data(
                        session, taskid, context=graph_data, csvwriter=taskwriter, csvf=csvf))
                )
            await asyncio.gather(*aiotasks)
        # https://github.com/aio-libs/aiohttp/issues/1115
        await asyncio.sleep(0)
        await asyncio.sleep(0)


async def write_data(session, taskid, context, csvwriter, csvf):
    queue = taskcluster.Queue(session=session)
    taskjson = await get_taskgraph_json(session, taskid, context)
    try:
        decision_task_status = await queue.status(taskid)
    except taskcluster.exceptions.TaskclusterRestFailure:
        return
    queue = None
    scheduled = decision_task_status['status']['runs'][-1]['scheduled']
    if not taskjson:
        return
    useful_tasks = find_relevant_tasks(taskjson, all_tasks=True)
    if not useful_tasks:
        return
    print('-')
    for task, attributes in useful_tasks.items():
        print('=')
        rows = await get_task_data_rows(session, task, attributes, context,
                                        scheduled)
        csvwriter.writerows(rows)
    print('%')


async def get_task_data_rows(session, taskid, attributes, context,
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
    print('.')
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
        row['product'] = context['product']
        row['version'] = context['version']
        row['display_version'] = context['display_version']
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
        else:
            row['locale'] = None
            rows.append(row)
    return rows

async def get_taskgraph_json(session, taskid, context):
    queue = taskcluster.Queue(session=session)
    artifact = {}
    try:
        artifact = await queue.getLatestArtifact(taskid, 'public/task-graph.json')
    except taskcluster.exceptions.TaskclusterRestFailure as e:
        pass
    return artifact


def find_relevant_tasks(task_graph_json, all_tasks=False):
    return {task: definition["attributes"]
            for task, definition in task_graph_json.items()}
    wanted_attributes = {'locale', 'chunk_locales', 'all_locales'}
    return {task: definition["attributes"]
            for task, definition in task_graph_json.items()
            if (all_tasks or
                (set(definition["attributes"].keys()) & wanted_attributes))
            }


def read_release_taskgraph_ids(repo_path):
    ret = {}
    for path in glob.glob(
        "{}/inflight/**/*.json".format(repo_path),
    ) + glob.glob(
        "{}/archive/**/*.json".format(repo_path),
    ):
        with open(path) as f:
            release_data = json.load(f)
        version = str(release_data['version'])
        gecko_version = re.match(r'\d*', version).group(0)
        if 'esr' in version:
            gecko_version += 'esr'
        product = release_data['product']
        graphs = []
        if 'inflight' in release_data:
            # RW-2 format
            for b in release_data['inflight']:
                if len(b['graphids']) == 0:
                    continue
                if isinstance(b['graphids'][0], list):
                    # Newest style, graphs are a list of 'phase', 'id'
                    for graph in b['graphids']:
                        graphs.append(graph[1])
                else:
                    # graphids are a flat list of graphs
                    graphs.extend(b['graphids'])
        elif 'builds' in release_data:
            for b in release_data['builds']:
                if 'graphid' in b:
                    graphs.append(b['graphid'])
                else:
                    # No graphid
                    pass
        else:
            raise NotImplementedError('Expected to have a search feature')
        for g in graphs:
            ret[g] = {'product': product, 'version': gecko_version,
                      'display_version': version}
    return ret

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
