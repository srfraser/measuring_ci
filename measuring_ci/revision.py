
import taskcluster


async def find_taskgroup_by_revision(
    revision, project, product, nightly=False,
):
    """Use the index to find a task group ID from a cset revision."""
    if nightly:
        nightly_index = "nightly."
    else:
        nightly_index = ""
    index = (  # collapse string
        "gecko.v2.{project}.{nightly}revision."
        "{revision}.{product}.linux64-opt"
    ).format(
        project=project,
        nightly=nightly_index,
        revision=revision,
        product=product,
    )

    idx = taskcluster.aio.Index()
    queue = taskcluster.aio.Queue()

    try:
        build_task = await idx.findTask(index)
        task_def = await queue.task(build_task['taskId'])
    except taskcluster.exceptions.TaskclusterRestFailure as e:
        print("Taskcluster error", e)
        return

    return task_def['taskGroupId']
