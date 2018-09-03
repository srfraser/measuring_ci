import csv
from datetime import datetime

import pandas as pd
import plotly.plotly as py
import plotly.figure_factory as ff
import plotly.graph_objs as go


KIND_ORDER = [
    'nightly-l10n', 'nightly-l10n-signing',
    'repackage-l10n', 'repackage-signing',
    'release-sign-and-push-langpacks',
    'release-beetmover-signed-langpacks',
    'partials', 'partials-signing', 'beetmover-repackage',
    'checksums', 'checksums-signing',
    'beetmover-checksums', 'balrog'
]


def main():
    raw_data = get_raw_data()

    relevant_rows = raw_data[
        # Filter out using Panda Boolean Masking
        (~raw_data['version'].str.contains('esr')) &  # not esr
        (~raw_data['build_platform'].str.contains('devedition')) &
        (~raw_data['build_platform'].str.contains('android')) &
        (~raw_data['build_platform'].str.contains('win64')) &
        (raw_data['build_platform'].str.contains('linux64')) &
        True
    ]
    traces = []
    for kind in sorted(relevant_rows['kind'].unique(),
                       key=lambda v: KIND_ORDER.index(v)):
        x_data = []
        y_data = []
        for version in sorted(relevant_rows['version'].unique()):
            timing = get_timing_by_kind(relevant_rows, version, kind)
            runtime = timing.apply(
                lambda r: (
                    datetime.strptime(r['resolved'], "%Y-%m-%dT%H:%M:%S.%fZ") -
                    datetime.strptime(r['started'], "%Y-%m-%dT%H:%M:%S.%fZ")).total_seconds(),
                axis=1)  # Series!
            if not runtime.empty:
                x_data.append(str(version))
                y_data.append(runtime.median())
        traces.append(go.Bar(
            x=x_data,
            y=y_data,
            name=kind
        ))
    layout = go.Layout(
        barmode='stack'
    )
    fig = go.Figure(data=traces, layout=layout)
    py.plot(fig, filename='stacked-bar')
    return 0
    if 0:
        traces = []
        for version in sorted(relevant_rows['version'].unique()):
            # min_date = get_min_scheduled(relevant_rows, version)
            for kind in relevant_rows['kind'].unique():
                timing = get_timing_by_kind(relevant_rows, version, kind)
                timing = timing.apply(
                    lambda r: (
                        datetime.strptime(r['resolved'], "%Y-%m-%dT%H:%M:%S.%fZ") -
                        datetime.strptime(r['started'], "%Y-%m-%dT%H:%M:%S.%fZ")).total_seconds(),
                    axis=1)  # Series!
                timing = pd.DataFrame(dict(times=timing, kind=kind))

                cached_timing = timing.copy()


def get_raw_data():
    return pd.read_csv('task_data.csv', sep='\t')


def complete_filter(rows):
    return (
        (rows['started'].notna()) &
        (rows['state'] == 'completed') &
        True
    )


def normalize_platform(rows, key):
        return (
            rows[key].str.replace(
                'nightly', ''
            ).str.replace(
                'devedition', ''
            ).str.rstrip('-')
        )


def get_min_scheduled(rows, version):
    return rows[
        complete_filter(rows) & normalize_platform(rows, 'build_platform') &
        (rows['version'] == version) &
        True
    ]['scheduled'].min()


def get_timing_by_kind(rows, version, kind):
    r = rows[
        complete_filter(rows) & normalize_platform(rows, 'build_platform') &
        (rows['version'] == version) &
        (rows['kind'] == kind)
    ]
    r.drop_duplicates(subset='taskid')
    return r


if __name__ == '__main__':
    main()
