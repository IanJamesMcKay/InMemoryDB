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


def read_runtimes(out_data, root_dir):
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
        durations = durations
        non_zero_durations = np.extract(durations > 0, durations)

        if len(non_zero_durations) == 0:
            print("  Skipping since non-zero durations are empty")
            out_data[name].append(0)
            continue

        out_data[name].append(np.min(non_zero_durations))


baseline_dir = "/home/moritz/pella/hyrise/joe/baseline"
adaptive_dir = "/home/moritz/pella/hyrise/joe/adaptive"
planning_dir = "/home/moritz/pella/hyrise/joe/planning"

baseline_iteration_files = [file_name for file_name in os.listdir(baseline_dir) if file_name.endswith(".Iterations.csv")]
adaptive_iteration_files = [file_name for file_name in os.listdir(adaptive_dir) if file_name.endswith(".Iterations.csv")]
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
read_runtimes(data, adaptive_dir)

avg_planning_durations = []
avg_baseline_execution_durations = []
avg_adaptive_execution_durations = []

for name in names:
    avg_planning_durations.append(data[name][0])
    avg_baseline_execution_durations.append(data[name][1])
    avg_adaptive_execution_durations.append(data[name][2])

avg_planning_durations = [d / 1000 for d in avg_planning_durations]
avg_baseline_execution_durations = [d / 1000 for d in avg_baseline_execution_durations]
avg_adaptive_execution_durations = [d / 1000 for d in avg_adaptive_execution_durations]

index = np.arange(len(names))

fig, ax = plt.subplots()

ax.set_ylabel('Time in ms')
ax.bar(index - 0.2, avg_planning_durations, 0.2, label="Generating 10 plans (avg)")
ax.bar(index, avg_baseline_execution_durations, 0.2, label="Execution - Baseline (min)")
ax.bar(index + 0.2, avg_adaptive_execution_durations, 0.2, label="Execution - Adaptive (min)")
ax.set_xticks(index)
ax.set_xticklabels(names, fontsize=3)
plt.legend()
plt.savefig("planning-baseline-adaptive.svg", format="svg")
#plt.show()
