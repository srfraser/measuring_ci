from datetime import datetime

import pandas as pd
import plotly.graph_objs as go
import plotly.plotly as py


def main():
    raw_data = get_raw_data()
    relevant_rows = raw_data[
        # Filter out using Panda Boolean Masking
        (raw_data['kind'] == 'nightly-l10n') &
        (~raw_data['build_platform'].str.contains('devedition')) &
        (~raw_data['build_platform'].str.contains('android')) &
        (raw_data['build_platform'].str.contains('win')) &
        # (raw_data['build_platform'].str.contains('win64')) &
        True
    ]
    raw_data = None
    data = []
    platforms = get_platforms(relevant_rows)
    for platform in sorted(platforms):
        relevant_rows.sort_values(by='date', inplace=True)
        data_line = go.Box(
            # y=get_runtime_totals_per_locale(relevant_rows, platform),
            y=get_runtime_totals_per_task(relevant_rows, platform),
            # x=get_weeks_per_locale(relevant_rows, platform),
            x=get_weeks_per_task(relevant_rows, platform),
            # x=get_days_per_locale(relevant_rows, platform),
            # x=get_days_per_task(relevant_rows, platform),
            name=platform,
        )
        data.append(data_line)
    # updatemenus = list([
    #     dict(
    #         active=0,
    #         buttons=list([
    #             dict(label='All',
    #                  method='update',
    #                  args = [{'visible': [True, False]},
    #                          {'title': 'All dates in past year'}]),
    #         ]),
    #     )
    # ])
    layout = {
        'xaxis': {
            'title': 'Week',
            'zeroline': False,
        },
        'yaxis': {
            'title': 'Job duration (per task) in seconds',
        },
        'boxmode': 'group',
        # 'updatemenus': updatemenus,
    }
    # import pdb;pdb.set_trace()
    add_gecko_values(layout)
    # data[-1] = go.Box(
    #     x=['2017-W50'],
    #     y=[0],
    #     text=["Gecko 60"],
    #     mode="text",
    # )

    fig = go.Figure(data=data, layout=layout)
    py.plot(fig, filename="win_desktop_l10n_duration_nightlies_per_task")


def add_gecko_values(layout):
    layout.setdefault('shapes', [])
    layout.setdefault('annotations', [])
    combos = [
        ('Gecko 63', '2018-W25'),
        ('Gecko 62', '2018-W18'),
        ('Gecko 61', '2018-W10'),
        ('Gecko 60', '2018-W03'),
        ('Gecko 59', '2017-W46'),
        ('Gecko 58', '2017-W38'),
        # ('Gecko 57', '2017-W31'),
    ]
    for gecko, week in combos:
        layout['shapes'].append({
            'type': 'line',
            'layer': 'below',
            'x0': week,
            'x1': week,
            'y0': 0,
            'y1': 1,
            'yref': 'paper',
            'line': {
                'color': 'green',
                'dash': 'dash',
            },
        })
        layout['annotations'].append(dict(
            x=week,
            y='1',
            showarrow=False,
            yref='paper',
            text=gecko,
        ))


def complete_filter(rows):
    return (
        (rows['started'].notna()) &
        (rows['state'] == 'completed') &
        True
    )


def get_runtime_totals_per_locale(rows, platform):
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"

    r = rows[
        complete_filter(rows) &
        (normalize_platform(rows, 'build_platform') == platform)
    ]
    value_counts = r['taskid'].value_counts()
    runtime = r.apply(
        lambda _r: (
            (datetime.strptime(_r['resolved'], fmt) -
             datetime.strptime(_r['started'], fmt)).total_seconds() /
            value_counts[_r['taskid']]
        ),
        axis=1)  # Series!
    return runtime


def get_runtime_totals_per_task(rows, platform):
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"

    r = rows[
        complete_filter(rows) &
        (normalize_platform(rows, 'build_platform') == platform)
    ]
    r = r.drop_duplicates(subset='taskid')
    runtime = r.apply(
        lambda _r: (
            datetime.strptime(_r['resolved'], fmt) -
            datetime.strptime(_r['started'], fmt)).total_seconds(),
        axis=1)  # Series!
    return runtime


def get_weeks_per_locale(rows, platform):
    r = rows[
        complete_filter(rows) &
        (normalize_platform(rows, 'build_platform') == platform)
    ]
    runtime = r.apply(
        lambda _r: (normalize_week(_r['date'])),
        axis=1)  # Series!
    return runtime


def get_weeks_per_task(rows, platform):
    r = rows[
        complete_filter(rows) &
        (normalize_platform(rows, 'build_platform') == platform)
    ]
    r = r.drop_duplicates(subset='taskid')
    runtime = r.apply(
        lambda _r: (normalize_week(_r['date'])),
        axis=1)  # Series!
    return runtime


def get_days_per_locale(rows, platform):
    r = rows[
        complete_filter(rows) &
        (normalize_platform(rows, 'build_platform') == platform)
    ]
    runtime = r.apply(
        lambda _r: (normalize_day(_r['date'])),
        axis=1)  # Series!
    return runtime


def get_days_per_task(rows, platform):
    r = rows[
        complete_filter(rows) &
        (normalize_platform(rows, 'build_platform') == platform)
    ]
    r = r.drop_duplicates(subset='taskid')
    runtime = r.apply(
        lambda _r: (normalize_day(_r['date'])),
        axis=1)  # Series!
    return runtime


def normalize_version(ver):
    return str(ver).replace('esr', '')


def normalize_week(date_str):
    from_fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
    to_fmt = '%Y-W%U'
    return datetime.strptime(date_str, from_fmt).strftime(to_fmt)


def normalize_day(date_str):
    from_fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
    to_fmt = '%Y-%m-%d'
    return datetime.strptime(date_str, from_fmt).strftime(to_fmt)


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
    return pd.read_csv('nightly_task_data.csv', sep='\t')


if __name__ == '__main__':
    main()
