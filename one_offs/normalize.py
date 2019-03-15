
"""
Normalise timestamps.

Take a set of log files, and change all the timestamps to be offsets from the
task start time. This lets us more easily compare the time individual steps
take.
"""

import datetime
import glob
import re

import pandas as pd


def normalize(logfile):
    output_filename = f"{logfile}.out"
    timestamp_format = r"^(\d{2}):(\d{2}):(\d{2})"

    today = datetime.datetime.now()
    first_ts = None
    zero = datetime.datetime(today.year, today.month, today.day, 0, 0, 0)
    with open(logfile) as f, open(output_filename, 'w') as o:
        for line in f:
            result = re.match(timestamp_format, line)
            if not result:
                continue
            hour, minute, second = [int(i) for i in result.groups()]
            ts = datetime.datetime(today.year, today.month, today.day, hour, minute, second)
            if not first_ts:
                first_ts = datetime.datetime(today.year, today.month,
                                             today.day, hour, minute, second)
            normalised = zero + (ts - first_ts)
            ts = f"{ts.hour:02}:{ts.minute:02}:{ts.second:02}"
            normalised = f"{normalised.hour:02}:{normalised.minute:02}:{normalised.second:02}"
            print(ts, normalised)
            line = line.replace(ts, normalised)
            o.write(line)


def main():
    for filename in glob.glob('*.log'):
        normalize(filename)

    results = list()
    for filename in glob.glob('*.log.out'):
        with open(filename) as f:
            data = [l.split()[0] for l in f]
            results.append(pd.DataFrame(data=pd.to_timedelta(data)))
    df = pd.concat(results)
    df.to_csv('log_distribution_results.csv')


if __name__ == "__main__":
    main()
