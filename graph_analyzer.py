import asyncio
import logging
import os

import pandas as pd
import yaml

from measuring_ci.artifacts import get_artifact_costs
from measuring_ci.costs import fetch_all_worker_costs, taskgraph_cost
from taskhuddler.aio.graph import TaskGraph

LOG_LEVEL = logging.INFO

# AWS artisinal log handling, they've already set up a handler by the time we get here
log = logging.getLogger()
log.setLevel(LOG_LEVEL)
# some modules are very chatty
logging.getLogger("taskcluster").setLevel(logging.INFO)
logging.getLogger("aiohttp").setLevel(logging.INFO)


def load_parquet(filename, columns):
    """Load existing parquet file or an empty one."""
    try:
        df = pd.read_parquet(filename)
        log.info("Loaded %s", filename)
    except Exception as exc:
        log.info("Couldn't load %s, using empty data set (%s)", filename, exc)
        df = pd.DataFrame(columns=columns)
    return df


async def analyze_taskgraph(args, config):

    log.info("Examining taskgraph %s", args['groupid'])
    graph = await TaskGraph(args['groupid'])

    log.info("Fetching worker costs")
    worker_costs = fetch_all_worker_costs(
        tc_csv_filename=config['costs_csv_file'],
        other_csv_filename=config.get('other_costs_csv_file'),
    )

    log.info("Calculating costs")

    raw_data = args['data']
    if not isinstance(raw_data, dict):
        raise ValueError("Only able to complete dictionaries. Wrong value passed as data")

    for key, value in raw_data.items():
        if value is not None:
            continue
        if key == "graph_date":
            raw_data[key] = graph.earliest_start_time.strftime("%Y-%m-%d")
        elif key == 'compute_time':
            raw_data[key] = graph.total_compute_time().total_seconds()
        elif key == 'taskcount':
            raw_data[key] = len([t for t in graph.tasks()])
        elif key in ['totalcost', 'idealcost']:
            results = taskgraph_cost(graph, worker_costs)
            for index, label in enumerate(['totalcost', 'idealcost']):
                if label in raw_data:
                    raw_data[label] = results[index]
        elif key in ['artifact_size', 'artifact_projected_cost']:
            results = await get_artifact_costs(graph)
            for index, label in enumerate(['artifact_size', 'artifact_projected_cost']):
                if label in raw_data:
                    raw_data[label] = results[index]

    # Need to convert scalar values to lists
    costs_df = pd.DataFrame.from_dict({k: [v] for k, v in raw_data.items()})

    # Split up things based on project, if mentioned.
    if 'project' in raw_data:
        config['staging_output'] = config['staging_output'].format(project=raw_data['project'])
    output = os.path.join(config['staging_output'], "{}.parquet".format(args['groupid']))
    log.info("Writing parquet file %s", output)
    costs_df.to_parquet(output, compression='gzip')


async def main(args):
    """What to do."""
    with open(args['config'], 'r') as yamlfile:
        config = yaml.load(yamlfile)
    os.environ['TC_CACHE_DIR'] = config['TC_CACHE_DIR']
    config['backfill_count'] = args.get('backfill_count', None)

    await analyze_taskgraph(args=args, config=config)


def lambda_handler(args, context):
    """AWS entrypoint."""
    assert context  # not current used
    if 'config' not in args:
        args['config'] = 'scanner.yml'
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))


if __name__ == "__main__":
    payload = {
        "config": "nightlies.yml",
        "groupid": "JypwL9OsRkq-hVsOERwBFA",  # Might not have the same name in the data
        "data": {
            "product": "firefox",
            "groupid": "FOV0o1yDTIyurXT6LHnBtQ",
            "revision": "revision",
            "graph_date": None,
            "version": "version",
            'totalcost': None,
            'idealcost': None,
            'taskcount': None,
            'compute_time': None,
            'artifact_size': None,
            'artifact_projected_cost': None,
        },
    }
    log.setLevel(logging.DEBUG)

    lambda_handler(payload, 'foo')
