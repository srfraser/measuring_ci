"""
Utilities for reading release graph data from ReleaseWarrior.

https://github.com/mozilla-releng/releasewarrior-data

Lots of assumptions here about the directory structure inside releasewarrior-data.
"""
import glob
import json
import os
from datetime import datetime

from sh import git


def clone_releasewarrior_data(dest_path="releasewarrior-data"):
    """Clone the releasewarrior-data repository.

    We're not interested in the history, so limit the depth of the clone.
    """
    if not dest_path.startswith('/'):
        dest_path = os.path.join(os.getcwd(), dest_path)
    if os.path.exists(dest_path):
        if os.path.exists(os.path.join(dest_path, '.git')):
            git('-C', dest_path, 'pull')
        else:
            raise ValueError('Path exists and is not a git repository: {}'.format(dest_path))
    else:
        git('clone', '--depth', '10', 'https://github.com/mozilla-releng/releasewarrior-data', dest_path)
    return dest_path


def fetch_release_data(json_path):
    """Extract useful fields from a releasewarrior data file."""
    with open(json_path) as f:
        release_data = json.load(f)

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


def read_release_taskgraph_ids(repo_path, since=None):
    """Find releasewarrior data files.

    Args:
        repo_path (str): Path to clone of releasewarrior-data repository.
        since (str|datetime): How much of the git history to examine. Default: all.
    """
    glob_files = list()
    for path in glob.glob("{}/inflight/**/*.json".format(repo_path)) + glob.glob("{}/archive/**/*.json".format(repo_path)):
        glob_files.append(path)

    if since:
        if isinstance(since, datetime):
            since = since.strftime("%Y-%m-%d")
        git_out = git('--no-pager', '-C', repo_path, 'log',
                      '--since="{}"'.format(since), '--name-only', '--pretty=format:')
        git_files = [os.path.join(repo_path, g) for g in git_out.splitlines()]
        paths = set(git_files) & set(glob_files)
    else:
        paths = glob_files

    graphs = dict()
    for path in paths:
        graphs.update(fetch_release_data(path))
    return graphs
