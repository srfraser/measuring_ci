
import argparse
import asyncio
from collections import defaultdict
from datetime import timedelta

import taskcluster.aio as taskcluster

from measuring_ci.costs import fetch_worker_costs
from taskhuddler.aio.graph import TaskGraph


def parse_args():
    parser = argparse.ArgumentParser(description="CI Costs")
    parser.add_argument('--taskgroupid', type=int)
    parser.add_argument('--revision', type=str)
    parser.add_argument('--nightly', action='store_true')
    parser.add_argument('--project', type=str, default='mozilla-central')
    parser.add_argument('--product', type=str, default='firefox')
    return parser.parse_args()


async def find_taskgroup_by_revision(revision, project, product, nightly=False):

    if nightly:
        nightly_index = "nightly."
    else:
        nightly_index = ""
    index = "gecko.v2.{project}.{nightly}revision.{revision}.{product}.linux64-opt".format(
        project=project,
        nightly=nightly_index,
        revision=revision,
        product=product,
    )
    print(index)
    idx = taskcluster.Index()
    queue = taskcluster.Queue()
    build_task = await idx.findTask(index)
    task_def = await queue.task(build_task['taskId'])

    return task_def['taskGroupId']


async def async_main():
    """Run all the async tasks."""
    args = parse_args()
    if args.taskgroupid:
        graph = await TaskGraph(args.taskgroupid)
    elif args.revision:
        graph_id = await find_taskgroup_by_revision(
            args.revision, args.project, args.product, args.nightly)
        print("Found {}".format(graph_id))
        graph = await TaskGraph(graph_id)

    total_wall_time_buckets = defaultdict(timedelta)

    for task in graph.tasks():
        key = task.json['status']['workerType']
        total_wall_time_buckets[key] += sum(task.run_durations(), timedelta(0))

    worker_type_costs = fetch_worker_costs('aws_cost_estimates.csv')

    total_cost = 0.0

    for bucket in total_wall_time_buckets:
        if bucket not in worker_type_costs:
            continue

        hours = total_wall_time_buckets[bucket].total_seconds() / (60 * 60)
        cost = worker_type_costs.at[bucket, 'unit_cost'] * hours

        total_cost += cost

    return total_cost


def main():
    """Manage the async loop."""
    loop = asyncio.get_event_loop()
    cost = loop.run_until_complete(async_main())
    loop.close()

    print(cost)


if __name__ == '__main__':
    print("To cache task graph results:")
    print("export TC_CACHE_DIR=/path/to/cache")
    main()
