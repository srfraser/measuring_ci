import asyncio
import json
import logging

import aiodns  # noqa
import aiohttp

from .files import open_wrapper
from .revision import find_taskgroup_by_revision

logging.basicConfig(level=logging.INFO)

log = logging.getLogger()


async def scan_pushlog(pushlog_url,
                       project='mozilla-central',
                       product='firefox',
                       starting_push=None,
                       cache_file=None):
    """Scan through the pushlog for entries.

    Args:
        pushlog_url (str): url template for pushlog, including {project}
        project (str): mozilla-central, releases/mozilla-release or similar
        product (str): Used for finding the taskgraph. e.g. 'firefox'
        starting_push (int): push ID to start from. Defaults to most recent 10
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
            with open_wrapper(cache_file, 'r') as f:
                pushes = json.load(f)
        except Exception as e:
            log.error(e)

    if pushes.get(project) and not starting_push:
        starting_push = max(pushes[project].keys())
        print("Setting starting_push to {}".format(starting_push))

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
        if starting_push:
            url += "&startID={}".format(starting_push)
        log.debug("Querying url %s", url)
        response = await session.get(url)
        new_pushes = await response.json()
        for push in new_pushes.get('pushes', list()):
            log.debug("Examining push %s", push)
            epoch = new_pushes['pushes'][push]['date']
            # This is the cset used for CI indexing.
            final_cset = new_pushes['pushes'][push]['changesets'][-1]

            graph_id = await find_taskgroup_by_revision(
                revision=final_cset,
                project=project,
                product=product,
            )
            if not graph_id:
                log.info("Couldn't find task graph for revision %s", final_cset)
                graph_id = ""
            pushes[project][push] = {
                "date": epoch,
                "changeset": final_cset,
                "taskgraph": graph_id,
            }
    if cache_file:
        with open_wrapper(cache_file, 'w') as f:
            json.dump(pushes, f, indent=4)
    return pushes
