import asyncio
import logging
import urllib.parse

import boto3
import pandas as pd
import yaml

from measuring_ci.utils import find_staged_data_files

LOG_LEVEL = logging.INFO

# Without this, urllib doesn't know about s3 and removes s3://netloc/
urllib.parse.uses_netloc.append('s3')
urllib.parse.uses_relative.append('s3')

# AWS artisinal log handling, they've already set up a handler by the time we get here
log = logging.getLogger()
log.setLevel(LOG_LEVEL)
# some modules are very chatty
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


def delete_parquet_files(url_list):
    """Remove a series of s3 objects, which must be in the same bucket."""
    s3_client = boto3.client('s3')
    bucket = urllib.parse.urlparse(url_list[0]).netloc
    delete_data = {
        'Objects': [{'Key': urllib.parse.urlparse(p).path.lstrip('/')} for p in url_list],
    }

    s3_client.delete_objects(Bucket=bucket, Delete=delete_data)


async def collate_parquet_files(args, config):
    """Collect parquet data files into one."""
    # Don't store the full path to integration/releases/...
    if 'project' in args:
        short_project = args['project'].split('/')[-1]
        config['total_cost_output'] = config['total_cost_output'].format(project=short_project)
        config['staging_output'] = config['staging_output'].format(project=args['project'])
    staged_files = await find_staged_data_files(config['staging_output'])
    url_parts = urllib.parse.urlparse(config['staging_output'])
    bucket_url = '{}://{}'.format(url_parts.scheme, url_parts.netloc)

    parquet_files = [
        urllib.parse.urljoin(bucket_url, path) for path in staged_files
    ]

    contents = [
        pd.read_parquet(p) for p in parquet_files
    ]
    existing_costs = load_parquet(
        filename=config['total_cost_output'],
        columns=[],
    )
    if not existing_costs.empty:
        contents.insert(0, existing_costs)
    new_costs = pd.concat(contents, sort=True)
    new_costs.drop_duplicates(subset=['groupid'], keep='last', inplace=True)
    log.info("Writing parquet file %s", config['total_cost_output'])
    new_costs.to_parquet(config['total_cost_output'], compression='gzip')

    log.info("Cleaning up")
    # Chunk deletes due to parameter size in s3 call
    chunk_size = 50
    for chunk_index in range(len(parquet_files))[::chunk_size]:
        chunk = parquet_files[chunk_index:chunk_index + chunk_size]
        delete_parquet_files(chunk)


async def main(args):
    """What to do."""
    with open(args['config'], 'r') as yamlfile:
        config = yaml.load(yamlfile)

    await collate_parquet_files(args=args, config=config)


def lambda_handler(args, context):
    """AWS entrypoint."""
    assert context  # not current used
    if 'config' not in args:
        args['config'] = 'scanner.yml'
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))


if __name__ == "__main__":
    payload = {
        "config": "scanner.yml",
        "project": "try",
    }
    log.setLevel(logging.DEBUG)

    lambda_handler(payload, 'foo')
