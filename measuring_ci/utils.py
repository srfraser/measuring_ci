import os


def tc_options():
    """Set Taskcluster options."""
    return {
        'rootUrl': os.environ.get('TASKCLUSTER_ROOT_URL', 'https://taskcluster.net'),
    }


async def semaphore_wrapper(semaphore, coro):
    async with semaphore:
        return await coro
