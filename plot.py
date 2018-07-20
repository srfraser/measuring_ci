import csv
from datetime import datetime

import plotly.plotly as py
import plotly.graph_objs as go


def main():
    raw_data = get_raw_data()

    relevant_rows = [row for row in raw_data
                     if ('beetmover' in row['kind'] and #row['kind'] == 'nightly-l10n' and
                         'esr' not in row['version'] and
                         'devedition' not in row['build_platform'] and
                         'android' not in row['build_platform'] and
                         # 'osx' in row['build_platform'] and
                         # 'win' in row['build_platform'] and
                         # 'linux' in row['build_platform'] and
                         # 'android' in row['build_platform'] and
                         True)]
    data = []
    platforms = get_platforms(relevant_rows)
    if 0:
        platforms = get_platforms(relevant_rows)
        for platform in sorted(platforms):
            data_line = go.Box(
                #y=get_runtime_totals_per_locale(relevant_rows, platform),
                y=get_runtime_totals_per_task(relevant_rows, platform),
                #x=get_versions_per_locale(relevant_rows, platform),
                x=get_versions_per_task(relevant_rows, platform),
                name=platform
            )
            data.append(data_line)
    for kind in sorted(set([row['kind'] for row in relevant_rows])):
        data_line = go.Box(
                #y=get_runtime_totals_per_locale(relevant_rows, platform),
                y=get_runtime_totals_per_task_beet(relevant_rows, kind),
                #x=get_versions_per_locale(relevant_rows, platform),
                x=get_versions_per_task_beet(relevant_rows, kind),
                name=kind
        )
        data.append(data_line)
    layout = {
        'xaxis': {
            'title': 'Gecko Version',
            'zeroline': False,
        },
        'yaxis': {
            'title': 'Job duration (per task) in seconds',
        },
        'boxmode': 'group',
    }
    fig = go.Figure(data=data, layout=layout)
    py.plot(fig, filename="beetmover_release_task_times")


def get_runtime_totals_per_locale(rows, platform):
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"

    def _filter(row):
        if (row.get('started') and row['state'] == 'completed' and
                normalize_platform(row['build_platform']) == platform):
            return True
        return False

    count_by_task = {}
    count_by_task.setdefault(0)
    for r in rows:
        if not _filter(r):
            continue
        count_by_task.setdefault(r['taskid'], 0)
        count_by_task[r['taskid']] += 1

    return [
        ((datetime.strptime(row['resolved'], fmt) -
          datetime.strptime(row['started'], fmt)).total_seconds() /
         float(count_by_task[row['taskid']])
         )
        for row in rows
        if _filter(row)
    ]


def get_runtime_totals_per_task(rows, platform):
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"

    def _filter(row):
        if (row.get('started') and row['state'] == 'completed' and
                normalize_platform(row['build_platform']) == platform):
            return True
        return False

    _seen = set()
    def _saw(row):
        nonlocal _seen
        if row['taskid'] in _seen:
            return True
        _seen |= {row['taskid']}
        return False

    return [
        ((datetime.strptime(row['resolved'], fmt) -
          datetime.strptime(row['started'], fmt)).total_seconds()
          )
        for row in rows
        if _filter(row) and (not _saw(row))
    ]


def get_runtime_totals_per_task_beet(rows, kind):
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"

    def _filter(row):
        if (row.get('started') and row['state'] == 'completed' and
                row['kind'] == kind):
            return True
        return False

    _seen = set()
    def _saw(row):
        nonlocal _seen
        if row['taskid'] in _seen:
            return True
        _seen |= {row['taskid']}
        return False

    return [
        ((datetime.strptime(row['resolved'], fmt) -
          datetime.strptime(row['started'], fmt)).total_seconds()
          )
        for row in rows
        if _filter(row) and (not _saw(row))
    ]

def get_versions_per_locale(rows, platform):
    return [
        normalize_version(row['version']) for row in rows
        if (row.get('started') and row['state'] == 'completed' and
            normalize_platform(row['build_platform']) == platform)
    ]


def get_versions_per_task(rows, platform):
    _seen = set()
    def _saw(row):
        nonlocal _seen
        if row['taskid'] in _seen:
            return True
        _seen |= {row['taskid']}
        return False

    return [
        normalize_version(row['version']) for row in rows
        if (row.get('started') and row['state'] == 'completed' and
            normalize_platform(row['build_platform']) == platform and
            not _saw(row))
    ]


def get_versions_per_task_beet(rows, kind):
    _seen = set()
    def _saw(row):
        nonlocal _seen
        if row['taskid'] in _seen:
            return True
        _seen |= {row['taskid']}
        return False

    return [
        normalize_version(row['version']) for row in rows
        if (row.get('started') and row['state'] == 'completed' and
            row['kind'] == kind and
            not _saw(row))
    ]


def normalize_version(ver):
    return str(ver).replace('esr', '')


def normalize_platform(platform):
    return platform.replace(
        'nightly', '').replace(
        'devedition', '').rstrip('-')


def get_platforms(rows):
    return {
        normalize_platform(row['build_platform'])
        for row in rows
        if row.get('started') and row['state'] == 'completed'
    }


def get_raw_data():
    # with open('task_data.bak.csv') as f:
    with open('task_data.csv') as f:
        return [row for row in csv.DictReader(f, delimiter='\t')]


if __name__ == '__main__':
    main()
