
import pandas as pd
import matplotlib.pyplot as plt


date_columns = ['start_timestamp', 'task_start_label',
                'suite_start', 'suite_end', 'task_end_label', 'end_timestamp']

df = pd.read_csv('autoland_test_logfiles.csv',
                 infer_datetime_format=True, parse_dates=date_columns)


# Create new columns

df['start_label_delay'] = df['task_start_label'] - df['start_timestamp']
df['suite_start_delay'] = df['suite_start'] - df['task_start_label']

# Graphs have difficulty with timedeltas
df['start_label_delay_s'] = df['start_label_delay'] / pd.Timedelta(seconds=1)
df['suite_start_delay_s'] = df['suite_start_delay'] / pd.Timedelta(seconds=1)

# Remove chunking to aggregate tasks a bit.
df['task_name_nochunks'] = df['task_name'].apply(lambda x: x.rstrip('1234567890-'))
df['total_duration'] = df['end_timestamp'] - df['task_start_label']
df['total_duration_s'] = df['total_duration'] / pd.Timedelta(seconds=1)
df['suite_delay_ratio'] = df['suite_start_delay_s'] / df['total_duration_s']
df['suite_delay_percentage'] = df['suite_delay_ratio'] * 100


def simple_display(df, column):
    show_start_delay = pd.DataFrame({col: vals[column]
                                     for col, vals in df.groupby('task_name_nochunks')})
    meds = show_start_delay.median().sort_values()
    meds = meds.tail(30)  # Last 30 values, or the graph is unreadable
    show_start_delay = pd.DataFrame(
        {col: vals[column] for col, vals in df.groupby('task_name_nochunks') if col in meds.index})
    # Recalculate for shorter list, as we need the index
    meds = show_start_delay.median().sort_values()
    show_sorted = show_start_delay[meds.index]
    show_sorted.boxplot(vert=False, sym='')
    plt.show()


simple_display(df, 'start_label_delay_s')
simple_display(df, 'suite_start_delay_s')


def display_with_task_count(df, column):
    fig, ax = plt.subplots()
    ax.set_ylabel('SUITE-START delay:Total Duration ratio')
    ax3 = ax.twinx()
    ax3.set_frame_on(True)
    ax3.patch.set_visible(False)
    ax3.set_ylabel('Task Count')
    show_start_delay = pd.DataFrame({col: vals[column]
                                     for col, vals in df.groupby('task_name_nochunks')})
    meds = show_start_delay.median().sort_values()
    meds = meds.tail(30)  # Last 30 values, or the graph is unreadable
    show_start_delay = pd.DataFrame(
        {col: vals[column] for col, vals in df.groupby('task_name_nochunks') if col in meds.index})
    # Recalculate for shorter list, as we need the index
    meds = show_start_delay.median().sort_values()
    show_sorted = show_start_delay[meds.index]
    show_sorted.boxplot(ax=ax, sym='', rot=90)
    task_counts = pd.DataFrame([row for _, row in df['task_name_nochunks'].value_counts(
    ).reset_index().iterrows() if row['index'] in meds.index])
    task_counts.set_index('index', inplace=True)
    tc = pd.DataFrame([task_counts.loc[col, 'task_name_nochunks'] for col in show_sorted.columns])
    tc.plot.line(ax=ax3, legend=False)
    plt.show()


display_with_task_count(df, 'start_label_delay_s')
display_with_task_count(df, 'suite_start_delay_s')
