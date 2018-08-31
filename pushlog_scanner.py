import os
import asyncio
import argparse
import csv

from collections import defaultdict
from datetime import datetime, timedelta

import yaml
import pandas as pd

import s3fs

from taskhuddler.aio.graph import TaskGraph

from measuring_ci.files import open_wrapper
from measuring_ci.revision import find_taskgroup_by_revision
from measuring_ci.pushlog import scan_pushlog


def fetch_worker_costs(csv_filename):
    """static snapshot of data from worker_type_monthly_costs table."""

    with open_wrapper(csv_filename, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # header
        return {row[1]: float(row[4]) for row in reader}


def parse_args():
    parser = argparse.ArgumentParser(description="CI Costs")
    parser.add_argument('--project', type=str, default='mozilla-central')
    parser.add_argument('--product', type=str, default='firefox')
    parser.add_argument('--config', type=str, default='scanner.yml')
    return parser.parse_args()


def probably_finished(timestamp):
    """Guess at whether a revision's CI tasks have finished by now."""
    timestamp = datetime.fromtimestamp(timestamp)
    # Guess that if it's been over 24 hours then all the CI tasks
    # have finished.
    if datetime.now() - timestamp > timedelta(days=1):
        return True
    return False


def taskgraph_full_cost(graph, costs_filename):
    total_wall_time_buckets = defaultdict(timedelta)

    for task in graph.tasks():
        key = task.json['status']['workerType']
        total_wall_time_buckets[key] += sum(task.run_durations(), timedelta(0))

    year = graph.earliest_start_time.year
    month = graph.earliest_start_time.month
    worker_type_costs = fetch_worker_costs(costs_filename)

    total_cost = 0.0

    for bucket in total_wall_time_buckets:
        if bucket not in worker_type_costs:
            continue

        hours = total_wall_time_buckets[bucket].total_seconds()/(60*60)
        cost = worker_type_costs[bucket] * hours

        total_cost += cost

    return total_cost


async def main():
    args = parse_args()

    with open(args.config, 'r') as y:
        config = yaml.load(y)
    os.environ['TC_CACHE_DIR'] = config['TC_CACHE_DIR']

    pushes = await scan_pushlog(config['pushlog_url'],
                                starting_push=34500,
                                cache_file=config['pushlog_cache_file'])
    tasks = list()

    try:
        existing_costs = pd.read_parquet(config['parquet_output'])
        print("Loaded existing costs: {}".format(existing_costs.describe()))
    except Exception as e:
        print("Couldn't load existing costs, using empty data set", e)
        existing_costs = pd.DataFrame(columns=['groupid', 'pushid', 'cost'])

    push_id_map = dict()
    for push in pushes:
        if push in existing_costs['pushid']:
            # Already examined this one.
            print("Already examined push {}, skipping.".format(push))
            continue
        if probably_finished(pushes[push]['date']):
            graph_id = await find_taskgroup_by_revision(
                revision=pushes[push]['changeset'],
                project=args.project,
                product=args.product
            )
            if not graph_id:
                print("Couldn't find graph id for push {}".format(push))
                continue
            print("Push {}, Graph ID: {}".format(push, graph_id))
            push_id_map[graph_id] = push
            tasks.append(asyncio.ensure_future(
                TaskGraph(graph_id)
            ))

    taskgraphs = await asyncio.gather(*tasks)

    costs = list()
    for graph in taskgraphs:
        costs.append([graph.groupid, push_id_map[graph.groupid], taskgraph_full_cost(graph, config['costs_csv_file'])])

    costs_df = pd.DataFrame(costs, columns=['groupid', 'pushid', 'cost'])

    new_costs = existing_costs.merge(costs_df, how='outer')

    new_costs.to_parquet(config['parquet_output'], compression='gzip')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
