import argparse
import asyncio
import copy
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import yaml

# from .measuring_ci.costs import fetch_all_worker_costs
# from .measuring_ci.pushlog import scan_pushlog
from taskhuddler.aio.graph import TaskGraph

LOG_LEVEL = logging.INFO

TASKGRAPHS = [
    "Iel3zFH9Sc64pTH2zqgEog",  # Promote
    "fXjUYLkoTkKZb_Uv4N2mKQ",  # Push
    "QaL2b6JPTta4oPCIQBnBNQ",  # Ship
]
CI_TASKGRAPH = "S-6-JjCzSBeDvbGOzIVlow"


# AWS artisinal log handling, they've already set up a handler by the time we get here
log = logging.getLogger()
log.setLevel(LOG_LEVEL)
# some modules are very chatty
logging.getLogger("taskcluster").setLevel(logging.INFO)
logging.getLogger("aiohttp").setLevel(logging.INFO)


def fetch_worker_costs(csv_filename):
    """Static snapshot of data from worker_type_monthly_costs table."""

    df = pd.read_csv(csv_filename)
    expect_columns = {
        "modified",
        "year",
        "month",
        "provider",
        "provisioner",
        "worker_type",
        "usage_hours",
        "cost",
    }

    if expect_columns.symmetric_difference(df.columns):
        raise ValueError("Expected worker_type_monthly_costs to have a specific set of columns.")

    # Sort newest first, ensures we keep current values
    df.sort_values(by=["year", "month"], ascending=False, inplace=True)
    df.drop_duplicates('worker_type', inplace=True)
    df['unit_cost'] = df['cost'] / df['usage_hours']
    df.set_index('worker_type', inplace=True)
    return df


def fetch_all_worker_costs(tc_csv_filename, scriptworker_csv_filename):
    df = fetch_worker_costs(tc_csv_filename)
    if scriptworker_csv_filename:
        sw_df = fetch_worker_costs(scriptworker_csv_filename)
        df = df.append(sw_df)
    return df


def parse_args():
    parser = argparse.ArgumentParser(description="CI Costs")
    parser.add_argument('--project', type=str, default='mozilla-release')
    parser.add_argument('--product', type=str, default='firefox')
    parser.add_argument('--config', type=str, default='scanner.yml')
    return parser.parse_args()


def probably_finished(timestamp):
    """Guess at whether a revision's CI tasks have finished by now."""
    timestamp = datetime.fromtimestamp(timestamp)
    # Guess that if it's been over 24 hours then all the CI tasks
    # have finished.
    if datetime.now() - timestamp > timedelta(days=1) or True:  # Assume finished
        return True
    return False


def taskgraph_cost(graph, worker_costs):
    total_wall_time_buckets = defaultdict(timedelta)
    final_task_wall_time_buckets = defaultdict(timedelta)
    for task in graph.tasks():
        key = task.json['status']['workerType']
        total_wall_time_buckets[key] += sum(task.run_durations(), timedelta(0))
        if task.completed:
            final_task_wall_time_buckets[key] += task.resolved - task.started

    total_cost = 0.0
    final_task_costs = 0.0

    for bucket in total_wall_time_buckets:
        if bucket not in worker_costs.index:
            continue

        hours = total_wall_time_buckets[bucket].total_seconds() / (60 * 60)
        cost = worker_costs.at[bucket, 'unit_cost'] * hours
        total_cost += cost

        hours = final_task_wall_time_buckets[bucket].total_seconds() / (60 * 60)
        cost = worker_costs.at[bucket, 'unit_cost'] * hours
        final_task_costs += cost

    return total_cost, final_task_costs


def find_push_by_group(group_id, project, pushes):
    return next(push for push in pushes[project] if pushes[project][push]['taskgraph'] == group_id)


async def _semaphore_wrapper(action, args, semaphore):
    async with semaphore:
        return await action(*args)


async def get_release_cost(product, config):
    """Scan a project's recent history for complete task graphs."""
    config = copy.deepcopy(config)

    semaphore = asyncio.Semaphore(10)
    tasks = list()

    for graph_id in TASKGRAPHS:
        log.debug("Examining graph %s", graph_id)
        tasks.append(asyncio.ensure_future(
            _semaphore_wrapper(
                TaskGraph,
                args=(graph_id,),
                semaphore=semaphore,
            )))

    log.info('Gathering task {} graphs'.format(len(tasks)))
    taskgraphs = await asyncio.gather(*tasks)

    release_cost = 0.0
    release_task_count = 0

    log.info('Calculating costs')
    worker_costs = fetch_all_worker_costs(
        tc_csv_filename=config['costs_csv_file'],
        scriptworker_csv_filename=config.get('costs_scriptworker_csv_file'),
    )
    for graph in taskgraphs:
        full_cost, final_runs_cost = taskgraph_cost(graph, worker_costs)
        task_count = len([t for t in graph.tasks()])
        release_task_count += task_count
        release_cost += full_cost
    print()
    print("Release 62.0.3 cost {} over {} tasks".format(release_cost, release_task_count))
    print()

    tasks = list()
    tasks.append(
        asyncio.ensure_future(
            _semaphore_wrapper(
                TaskGraph,
                args=(CI_TASKGRAPH,),
                semaphore=semaphore,
            )))
    ci_graph = await asyncio.gather(*tasks)
    full_cost, final_runs_cost = taskgraph_cost(ci_graph[0], worker_costs)
    task_count = len([t for t in ci_graph[0].tasks()])
    release_task_count += task_count
    release_cost += full_cost
    print("Including on-push CI Release 62.0.3 cost {} over {} tasks".format(release_cost, release_task_count))
    print()


async def main(args):
    with open(args['config'], 'r') as y:
        config = yaml.load(y)
    os.environ['TC_CACHE_DIR'] = config['TC_CACHE_DIR']
    config['backfill_count'] = args.get('backfill_count', None)

    await get_release_cost(args['product'], config)


def lambda_handler(args, context):
    # assert context  # not current used
    if 'config' not in args:
        args['config'] = 'scanner.yml'
    if 'product' not in args:
        args['product'] = 'firefox'
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))


if __name__ == '__main__':
    logging.basicConfig(level=LOG_LEVEL)
    # Use command-line arguments instead of json blob if not running in AWS Lambda
    lambda_handler(vars(parse_args()), {'dummy': 1})
