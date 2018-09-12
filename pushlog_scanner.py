import argparse
import asyncio
import csv
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import yaml

from measuring_ci.files import open_wrapper
from measuring_ci.pushlog import scan_pushlog
from taskhuddler.aio.graph import TaskGraph

logging.basicConfig(level=logging.INFO)

log = logging.getLogger()


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


def taskgraph_cost_final_runs_only(graph, costs_filename):
    total_wall_time_buckets = defaultdict(timedelta)

    for task in graph.tasks():
        key = task.json['status']['workerType']
        if task.completed:
            total_wall_time_buckets[key] += task.resolved - task.started

    worker_type_costs = fetch_worker_costs(costs_filename)

    total_cost = 0.0

    for bucket in total_wall_time_buckets:
        if bucket not in worker_type_costs:
            continue

        hours = total_wall_time_buckets[bucket].total_seconds() / (60 * 60)
        cost = worker_type_costs[bucket] * hours

        total_cost += cost

    return total_cost


def taskgraph_full_cost(graph, costs_filename):
    total_wall_time_buckets = defaultdict(timedelta)

    for task in graph.tasks():
        key = task.json['status']['workerType']
        total_wall_time_buckets[key] += sum(task.run_durations(), timedelta(0))

    worker_type_costs = fetch_worker_costs(costs_filename)

    total_cost = 0.0

    for bucket in total_wall_time_buckets:
        if bucket not in worker_type_costs:
            continue

        hours = total_wall_time_buckets[bucket].total_seconds() / (60 * 60)
        cost = worker_type_costs[bucket] * hours

        total_cost += cost

    return total_cost


def find_push_by_group(group_id, project, pushes):
    return next(push for push in pushes[project] if pushes[project][push]['taskgraph'] == group_id)


async def main(args):

    with open(args['config'], 'r') as y:
        config = yaml.load(y)
    os.environ['TC_CACHE_DIR'] = config['TC_CACHE_DIR']

    dataframe_columns = ['project', 'product', 'groupid', 'pushid', 'date', 'origin', 'totalcost', 'idealcost']

    args['short_project'] = args['project'].split('/')[-1]
    config['pushlog_cache_file'] = config['pushlog_cache_file'].format(project=args['project'].replace('/', '_'))

    pushes = await scan_pushlog(config['pushlog_url'],
                                project=args['project'],
                                product=args['product'],
                                # starting_push=107600,
                                cache_file=config['pushlog_cache_file'])
    tasks = list()

    try:
        existing_costs = pd.read_parquet(config['parquet_output'])
        print("Loaded existing costs")
        print(existing_costs.columns)
    except Exception as e:
        print("Couldn't load existing costs, using empty data set", e)
        existing_costs = pd.DataFrame(columns=dataframe_columns)

    for push in pushes[args['project']]:
        log.debug("Examining push %s", push)
        if str(push) in existing_costs['pushid'].values:
            log.info("Already examined push %s, skipping.", push)
            continue
        if probably_finished(pushes[args['project']][push]['date']):
            graph_id = pushes[args['project']][push]['taskgraph']
            if not graph_id or graph_id == '':
                log.info("Couldn't find graph id for push {}".format(push))
                continue
            log.info("Push %s, Graph ID: %s", push, graph_id)
            tasks.append(asyncio.ensure_future(
                TaskGraph(graph_id),
            ))
        else:
            print("Less than a day old, skipping")
    taskgraphs = await asyncio.gather(*tasks)

    costs = list()
    for graph in taskgraphs:
        push = find_push_by_group(graph.groupid, args['project'], pushes)
        costs.append(
            [
                args['short_project'],
                args['product'],
                graph.groupid,
                push,
                pushes[args['project']][push]['date'],
                'push',
                taskgraph_full_cost(graph, config['costs_csv_file']),
                taskgraph_cost_final_runs_only(graph, config['costs_csv_file']),
            ])

    costs_df = pd.DataFrame(costs, columns=dataframe_columns)

    new_costs = existing_costs.merge(costs_df, how='outer')
    log.info("Writing parquet file %s", config['parquet_output'])
    new_costs.to_parquet(config['parquet_output'], compression='gzip')


def lambda_handler(args, context):
    if 'config' not in args:
        args['config'] = 'scanner.yml'
    if 'product' not in args:
        args['product'] = 'firefox'
    if 'config' not in args:
        args['config'] = 'scanner.yml'
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))


if __name__ == '__main__':
    # Use command-line arguments instead of json blob if not running in AWS Lambda
    lambda_handler(vars(parse_args()), {})
