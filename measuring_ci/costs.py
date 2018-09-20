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

    df.drop_duplicates('worker_type', inplace=True)
    df['unit_cost'] = df['cost'] / df['usage_hours']
    df.set_index('worker_type', inplace=True)
    return df
