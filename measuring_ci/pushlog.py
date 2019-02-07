import asyncio
import json
import logging

import aiodns  # noqa
import aiohttp

from .files import open_wrapper
from .revision import find_taskgroup_by_revision

log = logging.getLogger()


async def scan_pushlog(pushlog_url,
                       project='mozilla-central',
                       product='firefox',
                       starting_push=None,
                       backfill_count=None,
                       cache_file=None):
    """Scan through the pushlog for entries.

    Args:
        pushlog_url (str): url template for pushlog, including {project}
        project (str): mozilla-central, releases/mozilla-release or similar
        product (str): Used for finding the taskgraph. e.g. 'firefox'
        starting_push (int): push ID to start from. Defaults to most recent 10
        backfill_count (int): number of older pushes to retrieve, prior to the oldest known push
        cache_file (str): Path to cached results. Understands s3:// syntax

    Returns:
        Flattened structure of:
        {
            "pushid": {
                "date": epoch time,
                "changeset": most recent changeset,
                "taskgraph": "task graph id"
            },
            ...
        }
    """
    pushes = dict()
    if cache_file:
        try:
            log.debug('Loading pushlog cache')
            with open_wrapper(cache_file, 'r') as f:
                pushes = json.load(f)
        except Exception as e:
            log.error(e)

    if pushes.get(project) and not starting_push:
        starting_push = max(pushes[project].keys())
        log.debug("Setting starting_push to {}".format(starting_push))

    if project not in pushes:
        pushes[project] = dict()

    loop = asyncio.get_event_loop()
    connector = aiohttp.TCPConnector(limit=100,
                                     resolver=aiohttp.resolver.AsyncResolver())
    timeout = aiohttp.ClientTimeout(total=60 * 60 * 3)

    async with aiohttp.ClientSession(loop=loop,
                                     connector=connector,
                                     timeout=timeout) as session:
        url = pushlog_url.format(project=project)
        if backfill_count:
            if starting_push:
                log.info('Backfilling {} earlier pushes'.format(backfill_count))
                first_known = int(min(pushes[project].keys()))
                url += "&startID={}&endID={}".format(first_known - backfill_count - 1,
                                                     first_known - 1)
            else:
                log.warning("Can't backfill until we have some pushlog data already cached, "
                            "ignoring backfill_count on this run and polling tipmost pushes")
        elif starting_push:
            url += "&startID={}".format(starting_push)
        log.debug("Querying push url %s", url)
        response = await session.get(url)
        new_pushes = await response.json()
        for push in new_pushes.get('pushes', list()):
            log.debug("Inspecting push %s", push)
            epoch = new_pushes['pushes'][push]['date']
            # This is the cset used for CI indexing.
            final_cset = new_pushes['pushes'][push]['changesets'][-1]

            graph_id = await find_taskgroup_by_revision(
                revision=final_cset,
                project=project,
                product=product,
            )
            if not graph_id:
                log.warning("Couldn't find task graph for {} revision {}".format(project,
                                                                                 final_cset))
                graph_id = ""
            pushes[project][push] = {
                "date": epoch,
                "changeset": final_cset,
                "taskgraph": graph_id,
            }
    if cache_file:
        with open_wrapper(cache_file, 'w') as f:
            json.dump(pushes, f, indent=4, sort_keys=True)
    return pushes
