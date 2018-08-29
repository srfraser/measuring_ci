import asyncio
import argparse

from collections import defaultdict
from datetime import datetime, timedelta

from taskhuddler.aio.graph import TaskGraph

from measuring_ci.revision import find_taskgroup_by_revision
from measuring_ci.pushlog import scan_pushlog

from cost_by_taskgraph import fetch_worker_costs

PUSHLOG_URL = 'https://hg.mozilla.org/{tree}/json-pushes?version=2'

# Optional: &full=1


def parse_args():
    parser = argparse.ArgumentParser(description="CI Costs")
    parser.add_argument('--project', type=str, default='mozilla-central')
    parser.add_argument('--product', type=str, default='firefox')
    return parser.parse_args()


def probably_finished(timestamp):
    """Guess at whether a revision's CI tasks have finished by now."""
    timestamp = datetime.fromtimestamp(timestamp)
    # Guess that if it's been over 24 hours then all the CI tasks
    # have finished.
    if datetime.now() - timestamp > timedelta(days=1):
        return True
    return False


def taskgraph_walltime(graph):
    total_wall_time_buckets = defaultdict(timedelta)

    for task in graph.tasks():
        key = task.json['status']['workerType']
        total_wall_time_buckets[key] += sum(task.run_durations(), timedelta(0))

    year = graph.earliest_start_time.year
    month = graph.earliest_start_time.month
    worker_type_costs = fetch_worker_costs(year, month)

    total_cost = 0.0

    for bucket in total_wall_time_buckets:
        if bucket not in worker_type_costs:
            continue

        hours = total_wall_time_buckets[bucket].total_seconds()/(60*60)
        cost = worker_type_costs[bucket] * hours

        total_cost += cost

    return total_cost


async def main():
    args = parse_args()

    pushes = await scan_pushlog(starting_push=34000, cache_file="./pushlog_cache.json")
    tasks = list()

    for push in pushes:
        if probably_finished(pushes[push]['date']):
            graph_id = await find_taskgroup_by_revision(
                revision=pushes[push]['changeset'],
                project=args.project,
                product=args.product
            )
            print("Push {}, Graph ID: {}".format(push, graph_id))
            tasks.append(asyncio.ensure_future(
                TaskGraph(graph_id)
            ))

    taskgraphs = await asyncio.gather(*tasks)
    costs = {graph.groupid: taskgraph_walltime(graph) for graph in taskgraphs}
    print(costs)


if __name__ == '__main__':
    print("To cache task graph results:")
    print("export TC_CACHE_DIR=/path/to/cache")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
