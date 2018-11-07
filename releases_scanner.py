import argparse
import asyncio
import copy
import logging
import os

import pandas as pd
import yaml

from measuring_ci.costs import fetch_all_worker_costs, taskgraph_cost
from measuring_ci.releasewarrior import read_release_taskgraph_ids
from taskhuddler.aio.graph import TaskGraph

LOG_LEVEL = logging.INFO

# AWS artisinal log handling, they've already set up a handler by the time we get here
log = logging.getLogger()
log.setLevel(LOG_LEVEL)
# some modules are very chatty
logging.getLogger("taskcluster").setLevel(logging.INFO)
logging.getLogger("aiohttp").setLevel(logging.INFO)


def parse_args():
    """Extract arguments."""
    parser = argparse.ArgumentParser(description="CI Costs")
    parser.add_argument('--config', type=str, default='releases.yml')
    return parser.parse_args()


async def _semaphore_wrapper(action, args, semaphore):
    """Wrap an async function with semaphores."""
    async with semaphore:
        return await action(*args)


def categorize_version(product, version):
    """Return a descriptive string of the release type.

    Perhaps this should be in mozilla_version
    """
    if product == 'devedition':
        return product
    elif 'b' in version:
        return 'beta'
    elif 'a' in version:
        return 'nightly'
    elif 'esr' in version:
        return 'esr'
    return 'release'


async def scan_releases(config):
    """Scan recent history for complete task graphs."""
    config = copy.deepcopy(config)
    cost_dataframe_columns = [
        'product', 'groupid',
        'graph_date', 'category',
        'phase', 'version', 'build_number',
        'totalcost', 'idealcost', 'taskcount',
    ]

    if not config['total_cost_output'].startswith('/'):
        config['total_cost_output'] = os.path.join(os.getcwd(), config['total_cost_output'])

    try:
        existing_costs = pd.read_parquet(config['total_cost_output'])
        log.info("Loaded existing release costs")
    except Exception:
        log.info("Couldn't load existing release costs, using empty data set")
        existing_costs = pd.DataFrame(columns=cost_dataframe_columns)

    log.info("Looking up taskgraph IDs")
    taskgraph_ids = read_release_taskgraph_ids(
        config['releasewarrior-data-repo'],
        config['github_token'],
    )
    log.info("Found %d taskgraph IDs", len(taskgraph_ids))

    tasks = list()
    semaphore = asyncio.Semaphore(10)

    for graph_id in taskgraph_ids:
        if str(graph_id) in existing_costs['groupid'].values:
            log.debug("Already examined taskgroup %s, skipping.", graph_id)
            continue
        tasks.append(asyncio.ensure_future(
            _semaphore_wrapper(
                TaskGraph,
                args=(graph_id,),
                semaphore=semaphore,
            )))

    log.info('Gathering task %d graphs', len(tasks))
    taskgraphs = await asyncio.gather(*tasks)

    costs = list()

    log.info('Calculating costs')
    worker_costs = fetch_all_worker_costs(
        tc_csv_filename=config['costs_csv_file'],
        scriptworker_csv_filename=config.get('costs_scriptworker_csv_file'),
    )
    for graph in taskgraphs:
        full_cost, final_runs_cost = taskgraph_cost(graph, worker_costs)
        product = taskgraph_ids[graph.groupid]['product']
        version = taskgraph_ids[graph.groupid]['version'].replace('rc', '')
        try:
            costs.append(
                [
                    product,
                    graph.groupid,
                    graph.earliest_start_time.strftime("%Y-%m-%d"),  # date bucket
                    categorize_version(product, version),
                    taskgraph_ids[graph.groupid]['phase'],
                    version,
                    taskgraph_ids[graph.groupid]['build_number'],
                    full_cost,
                    final_runs_cost,
                    len([t for t in graph.tasks()]),  # task count
                ])
        except Exception as e:
            log.warning('Something screwy with %s, skipping that graph: %s', graph.groupid, e)

    costs_df = pd.DataFrame(costs, columns=cost_dataframe_columns)

    new_costs = existing_costs.merge(costs_df, how='outer')
    log.info("Writing parquet file %s", config['total_cost_output'])
    new_costs.to_parquet(config['total_cost_output'], compression='gzip')


async def main(args):
    """Main program."""
    with open(args['config'], 'r') as cfg:
        config = yaml.load(cfg)
    os.environ['TC_CACHE_DIR'] = config['TC_CACHE_DIR']
    config['backfill_count'] = args.get('backfill_count', None)

    await scan_releases(config)


def lambda_handler(args, context):
    """AWS Lambda entry point."""
    assert context  # not currently used
    if 'config' not in args:
        args['config'] = 'releases.yml'
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))


if __name__ == '__main__':
    logging.basicConfig(level=LOG_LEVEL)
    # Use command-line arguments instead of json blob if not running in AWS Lambda
    lambda_handler(vars(parse_args()), {'dummy': 1})
