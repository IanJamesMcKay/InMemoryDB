import matplotlib.pyplot as plt
import sys
import os
import numpy as np
import re
from collections import defaultdict


def name_from_file_name(file_name):
    job_match = re.match(".*JOB-([0-9]+[a-z]+)", file_name)
    if job_match is not None:
        return job_match.group(1)
    return None


def read_runtimes(out_data, root_dir, iteration_limit = None):
    iteration_files = [file_name for file_name in os.listdir(root_dir) if file_name.endswith(".Iterations.csv")]

    for file_name in iteration_files:
        print("Processing {}".format(file_name))

        name = name_from_file_name(file_name)
        if name is None:
            print("  Skipping")
            continue

        file_path = os.path.join(root_dir, file_name)
        c = np.genfromtxt(file_path, names=True, delimiter=",")

        if "RankZeroPlanExecutionDuration" not in c.dtype.fields:
            print("  Skipping")
            continue

        durations = c["RankZeroPlanExecutionDuration"]
        if iteration_limit is not None:
            durations = durations[:iteration_limit]
        non_zero_durations = np.extract(durations > 0, durations)

        if len(non_zero_durations) == 0:
            print("  Skipping since non-zero durations are empty")
            out_data[name].append(0)
            continue

        out_data[name].append(np.min(non_zero_durations))


baseline_dir = "/home/moritz/pella/hyrise/joe/baseline"
planning_dir = "/home/moritz/pella/hyrise/joe/planning"

planning_iteration_files = [file_name for file_name in os.listdir(planning_dir) if file_name.endswith(".Iterations.csv")]

# Figure out the query names from the file names
names = []
for file_name in planning_iteration_files:
    name = name_from_file_name(file_name)
    if name is not None:
        names.append(name)
names = sorted(names, key=lambda n: (len(n), n))

#
data = defaultdict(list)

# Read planning durations
for file_name in planning_iteration_files:
    print("Processing {}".format(file_name))

    name = name_from_file_name(file_name)
    if name is None:
        print("  Skipping")
        continue

    file_path = os.path.join(planning_dir, file_name)
    c = np.genfromtxt(file_path, names=True, delimiter=",")

    if "PlanningDuration" not in c.dtype.fields:
        print("  Skipping")
        continue

    data[name].append(np.average(c["PlanningDuration"]))

read_runtimes(data, baseline_dir)

planning_execution_ratios = []

for name in names:
    planning_time, execution_time = data[name]
    print("{}: {} VS {}".format(name, planning_time, execution_time))
    planning_execution_ratios.append(planning_time/execution_time)

index = np.arange(len(names))
fig, ax = plt.subplots()

ax.set_ylabel('Ratio')
ax.bar(index, planning_execution_ratios, 0.5, label="Planning / Execution Ratio - Generating 1 Plan")
ax.set_xticks(index)
ax.set_xticklabels(names, fontsize=3)
plt.legend()
plt.savefig("planning-execution-ratio.svg", format="svg")
#plt.show()