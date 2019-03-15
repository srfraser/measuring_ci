"""
Download all the logfiles for tests from the given task graph.

"""

import asyncio
# import gzip
import json
import os
import re
from datetime import datetime

import numpy as np
import pandas as pd
from dateutil.parser import parse
from taskcluster.aio.queue import Queue
from taskcluster.exceptions import TaskclusterRestFailure

from taskhuddler.aio import TaskGraph

OUTPUT_FILE = 'autoland_test_logfiles.csv'

os.environ['TC_CACHE_DIR'] = 's3://mozilla-releng-metrics/taskgraph_cache/'
q = Queue({'rootUrl': 'https://taskcluster.net'})
log_artifact = 'public/logs/live_backing.log'

TERMS = {
    'Task ID: ': 'start_timestamp',
    '=== Task Starting ===': 'task_start_label',
    'SUITE-START': 'suite_start',
    'SUITE-END': 'suite_end',
    '=== Task Finished ===': 'task_end_label',
}
columns = [
    'task_id',
    'task_name',
    'start_timestamp',
    'task_start_label',
    'suite_start',
    'suite_end',
    'task_end_label',
    'end_timestamp',
]

TC_LINE = re.compile(r'^\[\w+ (\d+-\d+-\d+[T ]\d+:\d+:\d+)')


def analyze_logfile(logfile, task_id, task_name):
    data = dict()

    def extract_timestamp(line, previous=None):
        matches = TC_LINE.match(line)
        if matches:
            return parse(matches.groups()[0])

    data['task_id'] = task_id
    data['task_name'] = task_name
    timestamp = None
    zero = None
    for line in logfile.splitlines():
        if len(line.strip()) == 0:
            continue
        timestamp = extract_timestamp(line, previous=timestamp)
        if timestamp is None:
            continue
        if zero is None:
            zero = datetime(timestamp.year, timestamp.month, timestamp.day, 0, 0, 0)
        for term in TERMS.keys():
            if term in line:
                data[TERMS[term]] = timestamp

    data['end_timestamp'] = timestamp

    if not all([k in data for k in columns]):
        return

    return pd.DataFrame(
        [[data[k] for k in columns]],
        columns=columns)


async def fetch_logfile(task_id, task_name, semaphore):
    async with semaphore:
        print("Task fetching", task_id)
        try:
            r = await q.getLatestArtifact(task_id, log_artifact)
        except TaskclusterRestFailure:
            return
        logfile = await r['response'].text()
        # with gzip.open('./logfiles_bytask/{}.log'.format(task_id), 'w') as f:
        #    f.write(logfile)
        return analyze_logfile(logfile, task_id, task_name)


async def fetch_task_ids(groupid, semaphore):
    async with semaphore:
        print("TaskGraph: Fetching", groupid)
        g = await TaskGraph(groupid)
        return {t.task_id: t.name for t in g.tasks() if 'test' in t.name}


async def main():
    semaphore = asyncio.Semaphore(1)

    with open('groups.txt') as f:
        groups = [l.strip() for l in f]

    for group in groups:
        tasks = await fetch_task_ids(group, semaphore)
        with open('tasks.json', 'r') as f:
            data = json.load(f)
        data.update(tasks)
        with open('tasks.json', 'w') as f:
            f.write(json.dumps(data, indent=4))

    with open('tasks.json') as f:
        tasks = json.load(f)

    semaphore = asyncio.Semaphore(10)
    length = len(tasks) // 300 + 1
    first = False
    for chunk in np.array_split(list(tasks.keys()), length):
        if first:
            done = pd.DataFrame(columns=columns)
            first = False
        else:
            done = pd.read_csv(OUTPUT_FILE)
        done_task_ids = done['task_id'].values
        atasks = list()
        for task_id in chunk:
            if task_id in done_task_ids:
                continue
            atasks.append(asyncio.ensure_future(fetch_logfile(task_id, tasks[task_id], semaphore)))

        results = await asyncio.gather(*atasks)
        results.insert(0, done)
        results_df = pd.concat(results)
        results_df.reset_index()
        results_df.to_csv(OUTPUT_FILE, index=False)
        print("{} Updated {}".format(datetime.now(), OUTPUT_FILE))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
