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


def taskgraph_cost(graph, costs_filename):
    total_wall_time_buckets = defaultdict(timedelta)
    final_task_wall_time_buckets = defaultdict(timedelta)
    for task in graph.tasks():
        key = task.json['status']['workerType']
        total_wall_time_buckets[key] += sum(task.run_durations(), timedelta(0))
        if task.completed:
            final_task_wall_time_buckets[key] += task.resolved - task.started

    worker_type_costs = fetch_worker_costs(costs_filename)

    total_cost = 0.0
    final_task_costs = 0.0

    for bucket in total_wall_time_buckets:
        if bucket not in worker_type_costs:
            continue

        hours = total_wall_time_buckets[bucket].total_seconds() / (60 * 60)
        cost = worker_type_costs[bucket] * hours
        total_cost += cost

        hours = final_task_wall_time_buckets[bucket].total_seconds() / (60 * 60)
        cost = worker_type_costs[bucket] * hours
        final_task_costs += cost

    return total_cost, final_task_costs


def find_push_by_group(group_id, project, pushes):
    return next(push for push in pushes[project] if pushes[project][push]['taskgraph'] == group_id)


async def _semaphore_wrapper(action, args, semaphore):
    with semaphore:
        return await action(*args)


async def main(args):

    with open(args['config'], 'r') as y:
        config = yaml.load(y)
    os.environ['TC_CACHE_DIR'] = config['TC_CACHE_DIR']

    cost_dataframe_columns = [
        'project', 'product', 'groupid',
        'pushid', 'graph_date', 'origin',
        'totalcost', 'idealcost', 'taskcount',
    ]
    daily_dataframe_columns = ['project', 'product', 'ci_date', 'origin', 'totalcost', 'taskcount']

    args['short_project'] = args['project'].split('/')[-1]
    config['pushlog_cache_file'] = config['pushlog_cache_file'].format(
        project=args['project'].replace('/', '_'))

    pushes = await scan_pushlog(config['pushlog_url'],
                                project=args['project'],
                                product=args['product'],
                                # starting_push=34612,
                                cache_file=config['pushlog_cache_file'])
    tasks = list()

    try:
        existing_costs = pd.read_parquet(config['total_cost_output'])
        print("Loaded existing per-push costs")
        print(existing_costs.columns)
    except Exception as e:
        print("Couldn't load existing per-push costs, using empty data set", e)
        existing_costs = pd.DataFrame(columns=cost_dataframe_columns)

    semaphore = asyncio.Semaphore(10)

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
                _semaphore_wrapper,
                TaskGraph,
                args=(graph_id,),
                semaphore=semaphore,
            ))
        else:
            print("Less than a day old, skipping")
    taskgraphs = await asyncio.gather(*tasks)

    costs = list()
    daily_costs = defaultdict(int)
    daily_task_count = defaultdict(int)

    for graph in taskgraphs:
        push = find_push_by_group(graph.groupid, args['project'], pushes)
        full_cost, final_runs_cost = taskgraph_cost(graph, config['costs_csv_file'])
        task_count = len([t for t in graph.tasks()])
        date_bucket = graph.earliest_start_time.strftime("%Y-%m-%d")
        daily_costs[date_bucket] += full_cost
        daily_task_count[date_bucket] += task_count
        costs.append(
            [
                args['short_project'],
                args['product'],
                graph.groupid,
                push,
                pushes[args['project']][push]['date'],
                'push',
                full_cost,
                final_runs_cost,
                task_count,
            ])

    costs_df = pd.DataFrame(costs, columns=cost_dataframe_columns)

    new_costs = existing_costs.merge(costs_df, how='outer')
    log.info("Writing parquet file %s", config['total_cost_output'])
    new_costs.to_parquet(config['total_cost_output'], compression='gzip')

    try:
        daily_costs_df = pd.read_parquet(config['daily_totals_output'])
        print("Loaded existing daily totals")
    except Exception as e:
        print("Couldn't load existing daily totals, using empty data set", e)
        daily_costs_df = pd.DataFrame(columns=daily_dataframe_columns)

    dailies = list()
    for key in daily_costs:
        dailies.append([
            args['short_project'],
            args['product'],
            key,
            'push',
            daily_costs[key],
            daily_task_count.get(key, 0),
        ])
    new_daily_costs = pd.DataFrame(dailies, columns=daily_dataframe_columns)

    new_daily_costs.set_index(['project', 'product', 'ci_date', 'origin'], inplace=True)
    daily_costs_df.set_index(['project', 'product', 'ci_date', 'origin'], inplace=True)

    # Add new to old, using 0 as a filler if there's no matching index, then remove the
    # index so the columns operate as expected.
    daily_costs_df = daily_costs_df.add(new_daily_costs, fill_value=0).reset_index()

    log.info("Writing parquet file %s", config['daily_totals_output'])
    daily_costs_df.to_parquet(config['daily_totals_output'], compression='gzip')


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
