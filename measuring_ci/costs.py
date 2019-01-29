from collections import defaultdict
from datetime import timedelta

import pandas as pd


def fetch_worker_costs_all(csv_filename):
    """Static snapshot of data from worker_type_monthly_costs table."""

    df = pd.read_csv(csv_filename)
    expect_columns = {
        "modified",
        "year",
        "month",
        "provider",
        "provisioner",
        "worker_type",
        "usage_hours",
        "cost",
    }

    if expect_columns.symmetric_difference(df.columns):
        raise ValueError("Expected worker_type_monthly_costs to have a specific set of columns.")

    return df


def fetch_worker_costs(csv_filename):
    df = fetch_worker_costs_all(csv_filename)
    df['unit_cost'] = df['cost'] / df['usage_hours']
    # Adjustments so that method='nearest' will work.
    # It's a bit hacky that we're choosing the 15th of a month, but it gets us an approximation of
    # 'nearest' when we examine the date without adding first/last days of month as separate entries.
    df['epoch'] = pd.to_datetime(df.year * 10000 + df.month * 100 + 15,
                                 format='%Y%m%d').astype('int64') // 1e9
    df.set_index('epoch', inplace=True)
    df.sort_index(inplace=True)
    return df


def fetch_all_worker_costs(tc_csv_filename, scriptworker_csv_filename):
    df = fetch_worker_costs(tc_csv_filename)
    if scriptworker_csv_filename:
        sw_df = fetch_worker_costs(scriptworker_csv_filename)
        df = df.append(sw_df)
    return df


def worker_unit_cost(costs, worker_type, date):
    """Fetch the worker cost for the given year and month.

    Will use the nearest value if that combination is missing.
    """
    filter_1 = costs[costs['worker_type'] == worker_type]
    filter_1.drop_duplicates(subset=['year', 'month'], keep='last', inplace=True)
    return filter_1.iloc[filter_1.index.get_loc(date.timestamp(), method='nearest')]['unit_cost']


def taskgraph_cost(graph, worker_costs):
    """Calculate the cost of a taskgraph."""
    total_wall_time_buckets = defaultdict(timedelta)
    final_task_wall_time_buckets = defaultdict(timedelta)

    start_date = graph.earliest_start_time

    for task in graph.tasks():
        key = task.json['status']['workerType']
        total_wall_time_buckets[key] += sum(task.run_durations(), timedelta(0))
        if task.completed:
            final_task_wall_time_buckets[key] += task.resolved - task.started

    total_cost = 0.0
    final_task_costs = 0.0

    known_workers = worker_costs['worker_type'].unique()
    for bucket in total_wall_time_buckets:
        if bucket not in known_workers:
            continue
        hours = total_wall_time_buckets[bucket].total_seconds() / (60 * 60)
        unit_cost = worker_unit_cost(worker_costs, worker_type=bucket, date=start_date)
        cost = unit_cost * hours
        total_cost += cost

        hours = final_task_wall_time_buckets[bucket].total_seconds() / (60 * 60)
        unit_cost = worker_unit_cost(worker_costs, worker_type=bucket, date=start_date)
        cost = unit_cost * hours
        final_task_costs += cost

    return total_cost, final_task_costs
