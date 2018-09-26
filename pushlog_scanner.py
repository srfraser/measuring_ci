import argparse
import asyncio
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import yaml

from measuring_ci.costs import fetch_all_worker_costs
from measuring_ci.pushlog import scan_pushlog
from taskhuddler.aio.graph import TaskGraph

logging.basicConfig(level=logging.DEBUG)

log = logging.getLogger()


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


async def scan_project(project, product, config):

    cost_dataframe_columns = [
        'project', 'product', 'groupid',
        'pushid', 'graph_date', 'origin',
        'totalcost', 'idealcost', 'taskcount',
    ]
    daily_dataframe_columns = ['project', 'product', 'ci_date', 'origin', 'totalcost', 'taskcount']

    short_project = project.split('/')[-1]
    config['total_cost_output'] = config['total_cost_output'].format(project=short_project)
    config['daily_totals_output'] = config['daily_totals_output'].format(project=short_project)
    config['pushlog_cache_file'] = config['pushlog_cache_file'].format(
        project=project.replace('/', '_'))

    log.debug("Looking up pushlog")
    pushes = await scan_pushlog(config['pushlog_url'],
                                project=project,
                                product=product,
                                # starting_push=34612,
                                cache_file=config['pushlog_cache_file'])

    try:
        existing_costs = pd.read_parquet(config['total_cost_output'])
        log.info("Loaded existing per-push costs")
    except Exception as e:
        log.info("Couldn't load existing per-push costs, using empty data set", e)
        existing_costs = pd.DataFrame(columns=cost_dataframe_columns)

    semaphore = asyncio.Semaphore(10)
    tasks = list()

    for push in pushes[project]:
        log.debug("Examining push %s", push)
        if str(push) in existing_costs['pushid'].values:
            log.info("Already examined push %s, skipping.", push)
            continue
        if probably_finished(pushes[project][push]['date']):
            graph_id = pushes[project][push]['taskgraph']
            if not graph_id or graph_id == '':
                log.info("Couldn't find graph id for push {}".format(push))
                continue
            log.info("Push %s, Graph ID: %s", push, graph_id)
            tasks.append(asyncio.ensure_future(
                _semaphore_wrapper(
                    TaskGraph,
                    args=(graph_id,),
                    semaphore=semaphore,
                )))
        else:
            log.info("Push %s probably not finished, skipping", push)
    log.debug('Gathering task graphs')
    taskgraphs = await asyncio.gather(*tasks)

    costs = list()
    daily_costs = defaultdict(int)
    daily_task_count = defaultdict(int)

    log.debug('Calculating costs')
    worker_costs = fetch_all_worker_costs(
        tc_csv_filename=config['costs_csv_file'],
        scriptworker_csv_filename=config.get('costs_scriptworker_csv_file'),
    )
    for graph in taskgraphs:
        push = find_push_by_group(graph.groupid, project, pushes)
        full_cost, final_runs_cost = taskgraph_cost(graph, worker_costs)
        task_count = len([t for t in graph.tasks()])
        date_bucket = graph.earliest_start_time.strftime("%Y-%m-%d")
        daily_costs[date_bucket] += full_cost
        daily_task_count[date_bucket] += task_count
        costs.append(
            [
                short_project,
                product,
                graph.groupid,
                push,
                pushes[project][push]['date'],
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
        log.info("Loaded existing daily totals")
    except Exception as e:
        log.info("Couldn't load existing daily totals, using empty data set", e)
        daily_costs_df = pd.DataFrame(columns=daily_dataframe_columns)

    dailies = list()
    for key in daily_costs:
        dailies.append([
            short_project,
            product,
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


async def main(args):
    with open(args['config'], 'r') as y:
        config = yaml.load(y)
    os.environ['TC_CACHE_DIR'] = config['TC_CACHE_DIR']

    # cope with original style, listing one project, or listing multiple
    for project in args.get('projects', [args.get('project')]):
        await scan_project(project, args['product'], config)


def lambda_handler(args, context):
    assert context  # not current used
    if 'config' not in args:
        args['config'] = 'scanner.yml'
    if 'product' not in args:
        args['product'] = 'firefox'
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))


if __name__ == '__main__':
    # Use command-line arguments instead of json blob if not running in AWS Lambda
    lambda_handler(vars(parse_args()), {})
