import argparse
import asyncio
import logging
import os
import re
from datetime import date, datetime, timedelta

import boto3
import pandas as pd
import yaml

LOG_LEVEL = logging.INFO

# AWS artisinal log handling, they've already set up a handler by the time we get here
log = logging.getLogger()
log.setLevel(LOG_LEVEL)


def parse_args():
    """Extract arguments."""
    parser = argparse.ArgumentParser(description="CI Costs")
    parser.add_argument('--config', type=str, default=argparse.SUPPRESS)
    parser.add_argument('--month', type=int, default=None,
                        help="The month of costs to gather, as an integer where 0=Jan (default=<Last Full Month>)")
    parser.add_argument('--year', type=int, default=None,
                        help="The month of costs to gather, as an integer where 0=Jan (default=<most recent year of specified month>)")
    return parser.parse_args()


def iter_cost_and_usage_groups(config):
    ce = boto3.client(
        'ce',
        aws_access_key_id=config['TC_AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['TC_AWS_SECRET_ACCESS_KEY'],
    )
    next_page_token = None
    # Start Date -- Inclusive
    start_date = datetime(month=config['month'], year=config['year'], day=1)
    # End Date -- Exclusive
    # End always on first of month, but timedelta by enough days to always jump a month...
    end_date = (start_date + timedelta(days=32)).replace(day=1)
    while True:  # break below
        kwargs = dict(
            TimePeriod={
                'Start': start_date.strftime("%Y-%m-%d"),
                'End': end_date.strftime("%Y-%m-%d"),
            },
            Granularity='MONTHLY',  # MONTHLY, DAILY, HOURLY
            GroupBy=[{'Type': 'TAG', 'Key': 'WorkerType'}],
            Metrics=['UnblendedCost', 'UsageQuantity'],
            Filter={
                'Dimensions': {
                    'Key': 'USAGE_TYPE_GROUP',
                    'Values': ["EC2: Running Hours"],
                },
            },
        )
        if next_page_token:
            kwargs['NextPageToken'] = next_page_token
            next_page_token = None
        log.info("Getting a page of cost data from TC aws...")
        response = ce.get_cost_and_usage(**kwargs)
        log.debug(response['ResponseMetadata'])
        next_page_token = response.get('NextPageToken')
        # We don't expect multiple results by time per response
        assert len(response['ResultsByTime']) == 1
        yield from response['ResultsByTime'][0]['Groups']
        if not next_page_token:
            break


def split_worker_tag(tag):
    data = tag.split('/', maxsplit=1)
    if not data:
        raise ValueError("bad tag")
    if len(data) > 1:
        return data
    return ["none", data[0]]


async def fetch_raw_cost_explorer(config):
    worker_type_re = re.compile(re.escape(r'WorkerType$'))
    rows = []
    modified_str = datetime.now().isoformat(sep=" ")
    for group in iter_cost_and_usage_groups(config):
        # Expect only 1 key
        assert len(group['Keys']) == 1
        key = group['Keys'][0]
        if not worker_type_re.match(group['Keys'][0]):
            raise Exception("Unexpected Key in Group, {}".format(group))
        key = worker_type_re.sub('', key)
        if not key:
            key = "<untagged>"
        provisioner, worker_type = split_worker_tag(key)
        row = {
            'modified': modified_str,  # XXX Fixup
            'year': config['year'],
            'month': config['month'],
            'provider': 'aws',
            'provisioner': provisioner,
            'worker_type': worker_type,
            'usage_hours': group['Metrics']['UsageQuantity']['Amount'],
            'cost': group['Metrics']['UnblendedCost']['Amount'],
        }
        rows.append(row)
    df = pd.DataFrame(rows,
                      columns=['modified', 'year', 'month', 'provider', 'provisioner',
                               'worker_type', 'usage_hours', 'cost'])
    return df


async def update_worker_costs(config):
    df_ce = await fetch_raw_cost_explorer(config)
    log.info("Reading cost data from existing: {}".format(config['costs_csv_file']))
    df = pd.read_csv(config['costs_csv_file'])

    new_records = []
    for row in df_ce.itertuples(index=False):
        def boolean_mask_existing_row(df):
            worker_type_mask = (df['worker_type'] == row.worker_type)
            if row.worker_type == "<untagged>":
                # support old imported csv goop
                worker_type_mask = (
                    worker_type_mask |
                    (df['worker_type'] == 'No Tagkey: WorkerType')
                )
            return (
                (df['month'] == row.month) &
                (df['year'] == row.year) &
                (df['provider'] == row.provider) &
                (df['provisioner'] == row.provisioner) &
                (worker_type_mask)
            )

        # Set these again, warn if different
        overlapped_row_costs = df.loc[
            boolean_mask_existing_row,
            ('worker_type', 'usage_hours', 'cost'),
        ]
        if overlapped_row_costs.empty:
            new_records.append(row)
        else:
            update_tuple = (row.worker_type, row.usage_hours, row.cost)
            if tuple(overlapped_row_costs.values[0]) == update_tuple:
                # This test is a bit fragile, as it doesn't account for rounding differences such as float precision.
                continue
            # Not Empty, update for differences
            # use log.debug to avoid false positives in the warning.
            log.debug("Data differs and entry already exists for row `{}`, "
                      "updating existing `{}`".format(
                          row, overlapped_row_costs.to_dict(orient='records')[0]),
                      )
            df.loc[
                boolean_mask_existing_row,
                ('worker_type', 'usage_hours', 'cost'),
            ] = (row.worker_type, row.usage_hours, row.cost)

    new_df_records = pd.DataFrame.from_records(new_records,
                                               columns=['modified', 'year', 'month', 'provider', 'provisioner',
                                                        'worker_type', 'usage_hours', 'cost'])
    df = df.append(new_df_records)
    return df


async def main(args):
    """Main program."""
    with open(args['config'], 'r') as cfg:
        config = yaml.load(cfg)
    today = date.today()
    if args['month']:
        config['month'] = args['month']
    else:
        config['month'] = (today.replace(day=1) - timedelta(days=1)).month
    if args['year']:
        config['year'] = args['year']
    else:
        if today.replace(month=config['month'], day=1) >= today.replace(day=1):
            config['year'] = (today.replace(month=1, day=1) - timedelta(days=1)).year
        else:
            config['year'] = today.year

    # Enforce access keys for TC AWS
    if 'TC_AWS_ACCESS_KEY_ID' in os.environ and 'TC_AWS_SECRET_ACCESS_KEY' in os.environ:
        config['TC_AWS_ACCESS_KEY_ID'] = os.environ['TC_AWS_ACCESS_KEY_ID']
        config['TC_AWS_SECRET_ACCESS_KEY'] = os.environ['TC_AWS_SECRET_ACCESS_KEY']
    else:
        raise Exception(
            "TC_AWS_ACCESS_KEY_ID and TC_AWS_SECRET_ACCESS_KEY are required to be passed in environment")

    new_df = await update_worker_costs(config)
    log.info("Writing updated file to {}".format(config['costs_csv_file']))
    new_df.to_csv(config['costs_csv_file'], index=False)


def lambda_handler(args, context):
    """AWS Lambda entry point."""
    assert context  # not currently used
    if 'config' not in args:
        args['config'] = 'update_costs.yml'
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))


if __name__ == '__main__':
    logging.basicConfig(level=LOG_LEVEL)
    # Use command-line arguments instead of json blob if not running in AWS Lambda
    lambda_handler(vars(parse_args()), {'dummy': 1})
