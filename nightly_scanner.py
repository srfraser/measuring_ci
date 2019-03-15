import argparse
import asyncio
import copy
import json
import logging
import os
from datetime import datetime, timedelta

import pandas as pd

import boto3
import yaml
from measuring_ci.nightly import fetch_nightlies
from measuring_ci.utils import find_staged_data_files

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


async def find_examined_taskgraph_ids(config):
    """Find the task graph IDs we have examined already."""
    try:
        existing_costs = pd.read_parquet(config['total_cost_output'])
        taskgraphs = existing_costs['groupid'].tolist()
    except Exception:
        taskgraphs = list()

    staged_files = await find_staged_data_files(config['staging_output'])
    staged_taskgraphs = [os.path.basename(f).replace('.parquet', '') for f in staged_files]

    return taskgraphs + staged_taskgraphs


async def scan_nightlies(args, config):
    """Scan recent history for complete task graphs."""
    config = copy.deepcopy(config)

    examined_taskgraph_ids = await find_examined_taskgraph_ids(config)

    log.info("Looking up taskgraph IDs")
    nightlies = await fetch_nightlies(datetime.now() - timedelta(days=1))
    log.info("Found %d taskgraph IDs", len(nightlies))

    lambda_client = boto3.client('lambda')

    for graph_id in nightlies:
        if str(graph_id) in examined_taskgraph_ids:
            log.debug("Already examined taskgroup %s, skipping.", graph_id)
            continue
        args.update({
            'groupid': graph_id,
            'project': 'mozilla-central',
            'config': 'nightlies.yml',
            'data': {
                'product': nightlies[graph_id]['product'],
                'groupid': graph_id,
                'revision': nightlies[graph_id]['revision'],
                'graph_date': None,
                'version': nightlies[graph_id]['version'],
                'totalcost': None,
                'idealcost': None,
                'taskcount': None,
                'compute_time': None,
                'artifact_size': None,
                'artifact_projected_cost': None,
            },
        })
        log.info("Invoking lambda for %s", graph_id)
        lambda_client.invoke(
            FunctionName='taskgraph_analyzer',
            InvocationType='Event',
            Payload=json.dumps(args),
        )


async def main(args):
    """Main program."""
    with open(args['config'], 'r') as cfg:
        config = yaml.load(cfg)
    os.environ['TC_CACHE_DIR'] = config['TC_CACHE_DIR']
    config['backfill_count'] = args.get('backfill_count', None)

    await scan_nightlies(args, config)


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
