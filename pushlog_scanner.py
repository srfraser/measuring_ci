import argparse
import asyncio
import copy
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import yaml

from measuring_ci.artifacts import get_artifact_costs
from measuring_ci.costs import fetch_all_worker_costs, taskgraph_cost
from measuring_ci.pushlog import scan_pushlog
from measuring_ci.utils import semaphore_wrapper
from taskhuddler.aio.graph import TaskGraph

LOG_LEVEL = logging.INFO

# AWS artisinal log handling, they've already set up a handler by the time we get here
log = logging.getLogger()
log.setLevel(LOG_LEVEL)
# some modules are very chatty
logging.getLogger("taskcluster").setLevel(logging.INFO)
logging.getLogger("aiohttp").setLevel(logging.INFO)


def parse_args():
    """Parse arguments if run on command line."""
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


def find_push_by_group(group_id, project, pushes):
    """Find the correct push for a task graph."""
    return next(push for push in pushes[project] if pushes[project][push]['taskgraph'] == group_id)


def load_parquet(filename, columns):
    """Load existing parquet file or an empty one."""
    try:
        df = pd.read_parquet(filename)
        log.info("Loaded %s", filename)
    except Exception as exc:
        log.info("Couldn't load %s, using empty data set (%s)", filename, exc)
        df = pd.DataFrame(columns=columns)
    return df


async def fetch_taskgraphs_for_pushes(pushes, project, known_pushes):
    """Return TaskGraph objects for all provided pushes."""
    semaphore = asyncio.Semaphore(10)
    tasks = list()

    count_no_graph_id = 0
    count_not_finished = 0
    for push in pushes[project]:
        log.debug("Examining push %s", push)
        if str(push) in known_pushes:
            log.debug("Already examined push %s, skipping.", push)
            continue
        if probably_finished(pushes[project][push]['date']):
            graph_id = pushes[project][push]['taskgraph']
            if not graph_id or graph_id == '':
                log.debug("Couldn't find graph id for %s push %s", project, push)
                count_no_graph_id += 1
                continue
            log.debug("Push %s, Graph ID: %s", push, graph_id)
            tasks.append(asyncio.ensure_future(semaphore_wrapper(semaphore, TaskGraph(graph_id))))
        else:
            log.debug("Push %s probably not finished, skipping", push)
            count_not_finished += 1
    log.info('%d pushes without a graph_id; skipping %d probably not finished yet',
             count_no_graph_id, count_not_finished)

    log.info('Gathering task %d graphs', len(tasks))
    return await asyncio.gather(*tasks)


async def scan_project(project, product, config):
    """Scan a project's recent history for complete task graphs."""
    config = copy.deepcopy(config)
    cost_dataframe_columns = [
        'project', 'product', 'groupid',
        'pushid', 'graph_date', 'origin',
        'totalcost', 'idealcost', 'taskcount',
        'compute_time', 'artifact_size', 'artifact_projected_cost'
    ]
    daily_dataframe_columns = [
        'project', 'product', 'ci_date',
        'origin', 'totalcost', 'taskcount',
        'compute_time', 'artifact_size', 'artifact_projected_cost'
    ]

    short_project = project.split('/')[-1]
    config['total_cost_output'] = config['total_cost_output'].format(project=short_project)
    config['daily_totals_output'] = config['daily_totals_output'].format(project=short_project)
    config['pushlog_cache_file'] = config['pushlog_cache_file'].format(
        project=project.replace('/', '_'))

    log.info("Looking up pushlog for %s", project)
    pushes = await scan_pushlog(config['pushlog_url'],
                                project=project,
                                product=product,
                                # starting_push=34612,
                                backfill_count=config['backfill_count'],
                                cache_file=config['pushlog_cache_file'])

    existing_costs = load_parquet(config['total_cost_output'], cost_dataframe_columns)

    taskgraphs = await fetch_taskgraphs_for_pushes(pushes, project, existing_costs['pushid'].values)

    costs = list()
    daily_costs = defaultdict(int)
    daily_task_count = defaultdict(int)
    daily_time_used = defaultdict(timedelta)
    daily_artifact_size = defaultdict(int)
    daily_artifact_cost = defaultdict(int)

    log.info('Calculating costs')
    worker_costs = fetch_all_worker_costs(
        tc_csv_filename=config['costs_csv_file'],
        scriptworker_csv_filename=config.get('costs_scriptworker_csv_file'),
    )
    for graph in taskgraphs:
        push = find_push_by_group(graph.groupid, project, pushes)
        full_cost, final_runs_cost = taskgraph_cost(graph, worker_costs)
        artifact_size, artifact_cost = get_artifact_costs(graph)
        task_count = len([t for t in graph.tasks()])
        date_bucket = graph.earliest_start_time.strftime("%Y-%m-%d")
        daily_costs[date_bucket] += full_cost
        daily_task_count[date_bucket] += task_count
        daily_time_used[date_bucket] += graph.total_compute_time()
        daily_artifact_size[date_bucket] += artifact_size
        daily_artifact_cost[date_bucket] += artifact_cost
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
                graph.total_compute_time().total_seconds(),
                artifact_size,
                artifact_cost,
            ])

    costs_df = pd.DataFrame(costs, columns=cost_dataframe_columns)

    new_costs = existing_costs.merge(costs_df, how='outer')
    log.info("Writing parquet file %s", config['total_cost_output'])
    new_costs.to_parquet(config['total_cost_output'], compression='gzip')

    # Daily totals
    daily_costs_df = load_parquet(config['daily_totals_output'], daily_dataframe_columns)

    dailies = list()
    for key in daily_costs:
        dailies.append([
            short_project,
            product,
            key,
            'push',
            daily_costs[key],
            daily_task_count.get(key, 0),
            daily_time_used.get(key, timedelta(0)).total_seconds(),
        ])
    new_daily_costs = pd.DataFrame(dailies, columns=daily_dataframe_columns)

    new_daily_costs.set_index(['project', 'product', 'ci_date', 'origin'], inplace=True)
    daily_costs_df.set_index(['project', 'product', 'ci_date', 'origin'], inplace=True)

    # Add new to old, using 0 as a filler if there's no matching index, then remove the
    # index so the columns operate as expected.
    daily_costs_df = daily_costs_df.add(new_daily_costs, fill_value=0).reset_index()

    # sometimes we end up with float taskcounts
    daily_costs_df.taskcount = daily_costs_df.taskcount.astype(int)

    log.info("Writing parquet file %s", config['daily_totals_output'])
    daily_costs_df.to_parquet(config['daily_totals_output'], compression='gzip')


async def main(args):
    """What to do."""
    with open(args['config'], 'r') as yamlfile:
        config = yaml.load(yamlfile)
    os.environ['TC_CACHE_DIR'] = config['TC_CACHE_DIR']
    config['backfill_count'] = args.get('backfill_count', None)

    # cope with original style, listing one project, or listing multiple
    for project in args.get('projects', [args.get('project')]):
        await scan_project(project, args['product'], config)


def lambda_handler(args, context):
    """AWS entrypoint."""
    assert context  # not current used
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
