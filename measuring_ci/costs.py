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
    # Sort newest first, ensures we keep current values
    df.sort_values(by=["year", "month"], ascending=False, inplace=True)
    df.drop_duplicates('worker_type', inplace=True)
    df['unit_cost'] = df['cost'] / df['usage_hours']
    df.set_index('worker_type', inplace=True)
    return df


def fetch_all_worker_costs(tc_csv_filename, scriptworker_csv_filename):
    df = fetch_worker_costs(tc_csv_filename)
    if scriptworker_csv_filename:
        sw_df = fetch_worker_costs(scriptworker_csv_filename)
        df = df.append(sw_df)
    return df


def taskgraph_cost(graph, worker_costs):
    """Calculate the cost of a taskgraph."""
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
