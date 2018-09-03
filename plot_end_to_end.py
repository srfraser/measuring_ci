from datetime import datetime

import pandas as pd
import plotly.plotly as py
import plotly.graph_objs as go
import plotly.figure_factory as ff


def main():
    raw_data = get_raw_data()

    version = '62.0b7'
    reduced_data = raw_data[(raw_data['display_version'] == version) &
                            (raw_data['product'] == 'firefox')]

    kinds = get_kind_order(reduced_data)
    reduced_data = reduced_data[(
        (reduced_data['started'].notna()) &
        (reduced_data['state'] == 'completed') &
        True
    )]

    df = []
    for kind in kinds:
        local_data = reduced_data[reduced_data['kind'] == kind]
        platforms = get_platforms(local_data)
        for platform in platforms:
            if platform and 'source' not in platform:
                name = '{}[{}]'.format(kind, platform)
            else:
                name = kind
            df.append(dict(
                Task=name,
                Start=local_data[local_data['build_platform'] == platform]['started'].min(),
                Finish=local_data[local_data['build_platform'] == platform]['started'].max(),
            ))
    fig = ff.create_gantt(df)
    py.plot(fig, '62.0b7_gantt')
    return
    # import pdb;pdb.set_trace()
    data = []
    for platform in sorted(platforms):
        data_line = go.Box(
            y=get_runtime_totals_per_locale(reduced_data, platform),
            # y=get_runtime_totals_per_task(relevant_rows, platform),
            x=get_versions_per_locale(reduced_data, platform),
            # x=get_versions_per_task(relevant_rows, platform),
            name=platform,
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
    py.plot(fig, filename="desktop_l10n_duration_per_locale")


def get_kind_order(rows):
    _rows = rows.sort_values(by='started')
    # Reduce taskid's, keeping last (most recently run)
    _rows.drop_duplicates(subset='taskid', keep='last', inplace=True)
    # Now only first kind
    _rows.drop_duplicates(subset='kind', keep='first', inplace=True)
    # Return series of kinds
    return _rows['kind']


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


def normalize_version(ver):
    return str(ver).replace('esr', '')


def normalize_platform(rows, key):
        return (
            rows[key].str.replace(
                'nightly', '',
            ).str.replace(
                'devedition', '',
            ).str.rstrip('-')
        )


def get_platforms(rows):
    return normalize_platform(rows, 'build_platform').unique()


def get_raw_data():
    return pd.read_csv('task_data.csv', sep='\t',
                       converters={
                         'locale': str,
                         'build_platform': str,
                         'version': str,
                       })


if __name__ == '__main__':
    main()
