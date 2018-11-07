import argparse
import asyncio
import copy
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import yaml

from measuring_ci.costs import fetch_all_worker_costs, taskgraph_cost
from measuring_ci.nightly import fetch_nightlies
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
    parser.add_argument('--config', type=str, default='nightlies.yml')
    return parser.parse_args()


async def _semaphore_wrapper(action, args, semaphore):
    """Wrap an async function with semaphores."""
    async with semaphore:
        return await action(*args)


async def scan_nightlies(config):
    """Scan recent history for complete task graphs."""
    config = copy.deepcopy(config)
    cost_dataframe_columns = [
        'product', 'groupid',
        'revision', 'graph_date',
        'version', 'totalcost',
        'idealcost', 'taskcount',
    ]

    try:
        existing_costs = pd.read_parquet(config['total_cost_output'])
        log.info("Loaded existing nightly costs")
    except Exception:
        log.info("Couldn't load existing nightly costs, using empty data set")
        existing_costs = pd.DataFrame(columns=cost_dataframe_columns)

    log.info("Looking up taskgraph IDs")
    nightlies = await fetch_nightlies(datetime.now() - timedelta(days=1))
    log.info("Found %d taskgraph IDs", len(nightlies))

    tasks = list()
    semaphore = asyncio.Semaphore(10)

    for graph_id in nightlies:
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

    for graph in await asyncio.gather(*tasks):
        nightlies[graph.groupid]['graph'] = graph
    # Remove ones we're skipping as already processed.
    nightlies = {n: nightlies[n] for n in nightlies if 'graph' in nightlies[n]}

    costs = list()

    log.info('Calculating costs')
    worker_costs = fetch_all_worker_costs(
        tc_csv_filename=config['costs_csv_file'],
        scriptworker_csv_filename=config.get('costs_scriptworker_csv_file'),
    )
    for graph in nightlies:
        full_cost, final_runs_cost = taskgraph_cost(nightlies[graph]['graph'], worker_costs)

        costs.append(
            [
                nightlies[graph]['product'],
                graph,
                nightlies[graph]['revision'],
                nightlies[graph]['graph'].earliest_start_time.strftime("%Y-%m-%d"),  # date bucket
                nightlies[graph]['version'],
                full_cost,
                final_runs_cost,
                len([t for t in nightlies[graph]['graph'].tasks()]),  # task count
            ])

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

    await scan_nightlies(config)


def lambda_handler(args, context):
    """AWS Lambda entry point."""
    assert context  # not currently used
    if 'config' not in args:
        args['config'] = 'nightlies.yml'
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))


if __name__ == '__main__':
    logging.basicConfig(level=LOG_LEVEL)
    # Use command-line arguments instead of json blob if not running in AWS Lambda
    lambda_handler(vars(parse_args()), {'dummy': 1})
