import asyncio
import aiohttp
import aiodns
import json
import logging
import os

from datetime import datetime, timedelta

from cost_by_taskgraph import find_taskgroup_by_revision


PUSHLOG_URL = 'https://hg.mozilla.org/{project}/json-pushes?version=2'

# Optional: &full=1


logging.basicConfig(level=logging.INFO)

log = logging.getLogger()


async def scan_pushlog(project='mozilla-central',
                       starting_push=None,
                       cache_file=None):
    """Scan through the pushlog for entries.

    Args:
        project (str): mozilla-central, releases/mozilla-release or similar
        starting_push (int): push ID to start from. Defaults to most recent 10

    Returns:
        Flattened structure of:
        {
            "pushid": {
                "date": epoch time,
                "changeset": most recent changeset
            },
            ...
        }
    """
    pushes = dict()
    if cache_file:
        try:
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    pushes = json.load(f)
        except Exception as e:
            log.error(e)
            raise

    if pushes and not starting_push:
        starting_push = max(pushes.keys())
        print("Setting starting_push to {}".format(starting_push))

    loop = asyncio.get_event_loop()
    connector = aiohttp.TCPConnector(limit=100,
                                     resolver=aiohttp.resolver.AsyncResolver())
    timeout = aiohttp.ClientTimeout(total=60*60*3)
    results = dict()

    async with aiohttp.ClientSession(loop=loop,
                                     connector=connector,
                                     timeout=timeout) as session:
        url = PUSHLOG_URL.format(project=project)
        if starting_push:
            url += "&startID={}".format(starting_push)
        print(url)
        response = await session.get(url)
        new_pushes = await response.json()
        print(new_pushes)
        for push in new_pushes.get('pushes', list()):
            print(push)
            epoch = new_pushes['pushes'][push]['date']
            # This is the cset used for CI indexing.
            final_cset = new_pushes['pushes'][push]['changesets'][-1]
            pushes[push] = {
                "date": epoch,
                "changeset": final_cset,
            }
    if cache_file:
        with open(cache_file, 'w') as f:
            json.dump(pushes, f, indent=4)
    return pushes
