from os import listdir
import os
import sys
import pandas
import matplotlib.pyplot as plt
import re
import numpy as np
import interesting_job_queries


def diff(nocache, cache):
    return -((nocache - cache) / nocache) * 100.0


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

    plan_hashes = df["RankZeroPlanHash"]
    convergence_idx = df.shape[0] - 1 # last "counting" idx
    for idx in range(df.shape[0] - 1):
        if plan_hashes[idx] == plan_hashes[idx + 1]:
            convergence_idx = idx + 1
            break

    print("{} converges at iteration {}".format(name, convergence_idx))

    nocache_exe = df["RankZeroPlanExecutionDuration"][0] * iteration_count
    nocache_total = nocache_exe + df["PlanningDuration"][0]

    cache_planning_durations = df["PlanningDuration"][:convergence_idx]
    cache_cecaching_durations = df["CECachingDuration"][:convergence_idx]

    cache_exe = np.sum(df["RankZeroPlanExecutionDuration"])
    cache_total = cache_exe + np.sum(cache_planning_durations) + np.sum(cache_cecaching_durations)

    all_nocache_total += nocache_total
    all_cache_total += cache_total

    query_data = {
        "name": name,
        "nocache_exe": nocache_exe,
        "nocache_total": nocache_total,
        "cache_exe": cache_exe,
        "cache_total": cache_total,
        "diff": diff(nocache_total, cache_total)
    }

    table_data.append(query_data)

table_data = sorted(table_data, key=lambda query_data: query_data["diff"])

row_format = "{} & {:.2f}s & {:.2f}s & {:+.2f}\\% \\\\"


def print_selection(selection):
    for query_data in selection:
        nocache_total_seconds = query_data["nocache_total"] / 1000000
        cache_total_seconds = query_data["cache_total"] / 1000000

        print(row_format.format(query_data["name"], nocache_total_seconds,
                                                                cache_total_seconds, query_data["diff"]))

head_tail_count = 3

print_selection(table_data[:head_tail_count])
print("\hline")

print(pandas.DataFrame(table_data).to_string())

omitted = table_data[head_tail_count:-head_tail_count]
omitted_nocache_total = 0
omitted_cache_total = 0
for omitted_query in omitted:
    omitted_nocache_total += omitted_query["nocache_total"]
    omitted_cache_total += omitted_query["cache_total"]
omitted_nocache_total_seconds = omitted_nocache_total / 1000000
omitted_cache_total_seconds = omitted_cache_total / 1000000
omitted_diff = diff(omitted_nocache_total, omitted_cache_total)

print(row_format.format("{} omitted".format(len(omitted)), omitted_nocache_total_seconds, omitted_cache_total_seconds, omitted_diff))
print("\hline")
print_selection(table_data[-head_tail_count:])
print("\hline")

all_nocache_total_seconds = all_nocache_total / 1000000
all_cache_total_seconds = all_cache_total / 1000000

all_diff = diff(all_nocache_total, all_cache_total)
print(row_format.format("Accumulated", all_nocache_total_seconds, all_cache_total_seconds, all_diff))