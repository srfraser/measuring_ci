import argparse
import asyncio
import copy
import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
import yaml

from measuring_ci.pushlog import scan_pushlog
from measuring_ci.utils import find_staged_data_files

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


def fetch_taskgraphs_for_pushes(pushes, project, known_graphs):
    """Return TaskGraph objects for all provided pushes."""
    taskgraphs = list()

    count_no_graph_id = 0
    count_not_finished = 0
    for push in pushes[project]:
        log.debug("Examining push %s", push)

        if probably_finished(pushes[project][push]['date']):
            graph_id = pushes[project][push]['taskgraph']
            if not graph_id or graph_id == '':
                log.debug("Couldn't find graph id for %s push %s", project, push)
                count_no_graph_id += 1
                continue
            if graph_id in known_graphs:
                log.debug("Already examined push %s (%s), skipping.", push, graph_id)
                continue
            log.debug("Push %s, Graph ID: %s", push, graph_id)
            taskgraphs.append(graph_id)
        else:
            log.debug("Push %s probably not finished, skipping", push)
            count_not_finished += 1
    log.info('%d pushes without a graph_id; skipping %d probably not finished yet',
             count_no_graph_id, count_not_finished)

    return taskgraphs


async def scan_project(project, args, config):
    """Scan a project's recent history for complete task graphs."""
    config = copy.deepcopy(config)

    short_project = project.split('/')[-1]
    config['total_cost_output'] = config['total_cost_output'].format(project=short_project)
    config['pushlog_cache_file'] = config['pushlog_cache_file'].format(
        project=project.replace('/', '_'))
    config['staging_output'] = config['staging_output'].format(project=project)

    log.info("Looking up pushlog for %s", project)
    pushes = await scan_pushlog(config['pushlog_url'],
                                project=project,
                                product=args['product'],
                                starting_push=config['starting_push'],
                                backfill_count=config['backfill_count'],
                                cache_file=config['pushlog_cache_file'])

    examined_taskgraph_ids = await find_examined_taskgraph_ids(config)
    taskgraphs = fetch_taskgraphs_for_pushes(pushes, project, examined_taskgraph_ids)

    lambda_client = boto3.client('lambda')

    for graph_id in taskgraphs:
        push = find_push_by_group(graph_id, project, pushes)
        args.update({
            'groupid': graph_id,
            'data': {
                'project': project,
                'product': args['product'],
                'groupid': graph_id,
                'pushid': push,
                'graph_date': pushes[project][push]['date'],
                'origin': 'push',
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
    """What to do."""
    with open(args['config'], 'r') as yamlfile:
        config = yaml.load(yamlfile)
    os.environ['TC_CACHE_DIR'] = config['TC_CACHE_DIR']
    config['backfill_count'] = args.get('backfill_count', None)
    config['starting_push'] = args.get('starting_push', None)

    # cope with original style, listing one project, or listing multiple
    for project in args.get('projects', [args.get('project')]):
        await scan_project(project, args, config)


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
