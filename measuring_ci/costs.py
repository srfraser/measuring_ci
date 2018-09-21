import pandas as pd


def fetch_worker_costs(csv_filename):
    """Static snapshot of data from worker_type_monthly_costs table."""

    df = pd.read_csv(csv_filename)
    expect_columns = {
        "modified",
        "year",
        "month",
        "provider",
        "provisioner",
        "worker_type",
        "usage_hours",
        "cost",
    }

    if expect_columns.symmetric_difference(df.columns):
        raise ValueError("Expected worker_type_monthly_costs to have a specific set of columns.")

    # Sort newest first, ensures we keep current values
    df.sort_values(by=["year", "month"], ascending=False, inplace=True)
    df.drop_duplicates('worker_type', inplace=True)
    df['unit_cost'] = df['cost'] / df['usage_hours']
    df.set_index('worker_type', inplace=True)
    return df


def fetch_all_worker_costs(tc_csv_filename, scriptworker_csv_filename):
    df = fetch_worker_costs(tc_csv_filename)
    if scriptworker_csv_filename:
        sw_df = fetch_worker_costs(scriptworker_csv_filename)
        df = df.append(sw_df)
    return df
