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

blacklist = []

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
all_nocache_noexe = 0
all_cache_total = 0
all_cache_noexe = 0
accumulated_convergence_idx = 0

for name, df in data_frames:
    if df["RankZeroPlanExecutionDuration"][df.shape[0] - 1] == 0:
        print("{} converged on a timeout plan".format(name))

    if np.any(df["RankZeroPlanExecutionDuration"] == 0):
        print("{} timed out at iteration {}".format(name, np.argmax(df["RankZeroPlanExecutionDuration"] == 0)))
        continue

    plan_hashes = df["RankZeroPlanHash"]
    convergence_idx = df.shape[0] - 1 # last "counting" idx
    for idx in range(df.shape[0] - 1):
        if plan_hashes[idx] == plan_hashes[idx + 1]:
            convergence_idx = idx + 1
            break

    accumulated_convergence_idx += convergence_idx

    print("{} converges at iteration {}".format(name, convergence_idx))

    nocache_exe = df["RankZeroPlanExecutionDuration"][0] * iteration_count
    nocache_total = nocache_exe + df["PlanningDuration"][0]

    cache_planning_durations = df["PlanningDuration"][:convergence_idx]
    cache_cecaching_durations = df["CECachingDuration"][:convergence_idx]

    cache_exe = np.sum(df["RankZeroPlanExecutionDuration"])
    cache_total = cache_exe + np.sum(cache_planning_durations) + np.sum(cache_cecaching_durations)
    nocache_noexe = nocache_total - nocache_exe
    cache_noexe = cache_total - cache_exe

    all_nocache_total += nocache_total
    all_cache_total += cache_total
    all_cache_noexe += cache_noexe
    all_nocache_noexe += nocache_noexe

    query_data = {
        "name": name,
        "convergence_idx": convergence_idx,
        "nocache_exe": nocache_exe,
        "nocache_noexe": nocache_noexe,
        "nocache_total": nocache_total,
        "cache_exe": cache_exe,
        "cache_noexe": cache_noexe,
        "cache_total": cache_total,
        "diff_total": diff(nocache_total, cache_total),
        "diff_noexe": diff(nocache_noexe, cache_noexe)
    }

    table_data.append(query_data)

table_data = sorted(table_data, key=lambda query_data: query_data["diff_total"])
print(pandas.DataFrame(table_data).to_string())


row_format = "{} & {:.2f}s ({:.2f}s) & {:.2f}s ({:.2f}s) & {:+.2f}\\% & {} \\\\"


def print_selection(selection):
    for query_data in selection:
        nocache_total_seconds = query_data["nocache_total"] / 1000000
        cache_total_seconds = query_data["cache_total"] / 1000000
        nocache_noexe_seconds = query_data["nocache_noexe"] / 1000000
        cache_noexe_seconds = query_data["cache_noexe"] / 1000000

        print(row_format.format(query_data["name"],
                                nocache_total_seconds,
                                nocache_noexe_seconds,
                                cache_total_seconds,
                                cache_noexe_seconds,
                                query_data["diff_total"],
                                query_data["convergence_idx"]))

head_tail_count = 4

print_selection(table_data[:head_tail_count])
print("\hline")

omitted = table_data[head_tail_count:-head_tail_count]
omitted_aggregate = {
    "name": "{} omitted".format(len(omitted)),
    "convergence_idx": 0,
    "nocache_exe": 0,
    "nocache_noexe": 0,
    "nocache_total": 0,
    "cache_exe": 0,
    "cache_noexe": 0,
    "cache_total": 0,
    "diff_total": 0,
    "diff_noexe": 0
}

for omitted_query in omitted:
    omitted_aggregate["convergence_idx"] += omitted_query["convergence_idx"]
    omitted_aggregate["nocache_exe"] += omitted_query["nocache_exe"]
    omitted_aggregate["nocache_noexe"] += omitted_query["nocache_noexe"]
    omitted_aggregate["nocache_total"] += omitted_query["nocache_total"]
    omitted_aggregate["cache_exe"] += omitted_query["cache_exe"]
    omitted_aggregate["cache_noexe"] += omitted_query["cache_noexe"]
    omitted_aggregate["cache_total"] += omitted_query["cache_total"]
omitted_aggregate["convergence_idx"] /= len(omitted)
omitted_aggregate["convergence_idx"] = "{:.2f} avg".format(omitted_aggregate["convergence_idx"])
omitted_aggregate["diff_total"] = diff(omitted_aggregate["nocache_total"], omitted_aggregate["cache_total"])

print_selection([omitted_aggregate])

print("\hline")
print_selection(table_data[-head_tail_count:])
print("\hline")

all_nocache_total_seconds = all_nocache_total / 1000000
all_cache_total_seconds = all_cache_total / 1000000
all_nocache_noexe_seconds = all_nocache_noexe / 1000000
all_cache_noexe_seconds = all_cache_noexe / 1000000

all_diff = diff(all_nocache_total, all_cache_total)
all_convergence_idx = accumulated_convergence_idx / len(table_data)

print(row_format.format("Accumulated",
                        all_nocache_total_seconds,
                        all_nocache_noexe_seconds,
                        all_cache_total_seconds,
                        all_cache_noexe_seconds,
                        all_diff,
                        "{:.2f} avg".format(all_convergence_idx)))