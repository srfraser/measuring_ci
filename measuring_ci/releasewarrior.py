"""
Utilities for reading release graph data from ReleaseWarrior.

https://github.com/mozilla-releng/releasewarrior-data

Lots of assumptions here about the directory structure inside releasewarrior-data.
"""
import json
from datetime import datetime, timedelta

from github import Github


def fetch_release_data(file_contents):
    """Extract useful fields from a releasewarrior data file."""

    release_data = json.loads(file_contents)

    # print("release_data[version]: ", release_data['version'])
    # version = FirefoxVersion.parse(release_data['version'])
    version = release_data['version']
    product = release_data['product']

    unknown_phase = 'Unknown'  # For when no graph type is recorded.

    graphs = dict()
    if 'inflight' in release_data:
        # RW-2 format
        for build in release_data['inflight']:
            if not build['graphids']:
                continue
            if isinstance(build['graphids'][0], list):
                # Newest style, graphs are a list of ['phase', 'id']
                for graph in build['graphids']:
                    graphs[graph[1]] = {
                        'product': product,
                        'version': version,
                        'phase': graph[0],
                        'build_number': build.get('buildnum', 1),
                    }
            else:
                # graphids are a flat list of graphs
                for graph in build['graphids']:
                    graphs[graph] = {
                        'product': product,
                        'version': version,
                        'phase': unknown_phase,
                        'build_number': build.get('buildnum', 1),
                    }
    elif 'builds' in release_data:
        # Older RW-1 format.
        for build in release_data['builds']:
            if 'graphid' in build:
                graphs[build['graphid']] = {
                    'product': product,
                    'version': version,
                    'phase': unknown_phase,
                    'build_number': build.get('buildnum', 1),
                }
    else:
        raise NotImplementedError('Expected to have a search feature')

    return graphs


def read_release_taskgraph_ids(repository='mozilla-releng/releasewarrior-data', token=None):
    """Find releasewarrior data files.

    Args:
        repository (str): organisation/repo of releasewarrior-data repository.
        since (str|datetime): How much of the git history to examine. Default: all.
    """
    since = datetime.now() - timedelta(days=6)

    if token:
        g = Github(token)
    else:
        g = Github()  # May encounter rate limits.
    repo = g.get_repo(repository)
    commits = repo.get_commits(since=since)

    paths = set()
    for commit in commits:
        for file_obj in list(commit.files):
            if not file_obj.filename.endswith('.json'):
                continue
            if not file_obj.filename.startswith('archive'):  # Guarantees completed taskgraphs.
                continue
            paths.add(file_obj.filename)

    graphs = dict()
    for path in paths:
        graphs.update(fetch_release_data(
            repo.get_file_contents(path).decoded_content,
        ))
    return graphs
