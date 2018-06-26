from os import listdir
import os
import sys
import pandas
import matplotlib.pyplot as plt
import re
import numpy as np
import interesting_job_queries

use_whitelist = False
root_directory = "/home/moritz/pella/hyrise/joe/adaptive"

blacklist = ["16d", "16b", "17e"]

evaluation_name = os.path.split(root_directory)[-1]

if use_whitelist:
    query_whitelist = interesting_job_queries.interesting_job_queries()
else:
    query_whitelist = None

file_names = [filename for filename in listdir(root_directory) if filename.endswith("Iterations.csv")]
iteration_count = None
data_frames = []

for file_name in file_names:
    print("Reading {}".format(file_name))

    job_match = re.match(".*JOB-([0-9]+[a-z]+)", file_name)
    if job_match is None:
        print("  Skipping")
        continue

    name = job_match.group(1)
    if query_whitelist is not None and name not in query_whitelist:
        print("Skipping {} because of whitelist".format(name))
        continue

    try:
        df = pandas.read_csv(os.path.join(root_directory, file_name), sep=",")
        data_frames.append((name, df))
        if iteration_count is None:
            iteration_count = df.shape[0]
        else:
            assert iteration_count == df.shape[0]
    except Exception as e:
        print("Couldn't parse {}: {}".format(name, e))
        continue

print("Iteration Count: {}".format(iteration_count))

table_data = []

all_nocache_total = 0
all_cache_total = 0

for name, df in data_frames:
    if name in blacklist:
        continue

    nocache_exe = df["RankZeroPlanExecutionDuration"][0] * iteration_count
    cache_exe = np.sum(df["RankZeroPlanExecutionDuration"])
    nocache_total = nocache_exe + df["PlanningDuration"][0]
    cache_total = cache_exe + np.sum(df["PlanningDuration"]) + np.sum(df["CECachingDuration"])

    all_nocache_total += nocache_total
    all_cache_total += cache_total

    query_data = {
        "name": name,
        "nocache_exe": nocache_exe,
        "nocache_total": nocache_total,
        "cache_exe": cache_exe,
        "cache_total": cache_total,
        "diff": -((nocache_total - cache_total) / nocache_total) * 100.0
    }

    table_data.append(query_data)

table_df = pandas.DataFrame(table_data)
table_df = table_df.sort_values("diff")

print(table_df)

all_diff = -((all_nocache_total - all_cache_total) / all_nocache_total) * 100.0
print("{} {} {}".format(all_nocache_total, all_cache_total,all_diff ))