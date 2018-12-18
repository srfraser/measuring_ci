"""Utilities for reading release graph data from ShipIt."""
import requests


def fetch_shipit_taskgraph_ids(api_url='https://shipit-api.mozilla-releng.net/releases'):
    """Find release taskgraph IDs from ShipIt.

    https://shipit-api.mozilla-releng.net/releases?branch=releases%2Fmozilla-release&status=shipped
    """

    release_projects = [
        'releases/mozilla-release',
        'releases/mozilla-esr60',
        'releases/mozilla-beta',
    ]
    graphs = dict()
    for project in release_projects:
        query = {
            'status': 'shipped',
            'branch': project,
        }
        response = requests.get(api_url, params=query)
        response.raise_for_status()
        for release in response.json():

            for phase in release['phases']:
                # The action task id will be the graphid of the task graph it creates.
                graphs[phase['actionTaskId']] = {
                    'build_number': release['build_number'],
                    'version': release['version'],
                    'product': release['product'],
                    'phase': phase['name'].split('_')[0],  # ship_fennec -> ship
                }

    return graphs
