import matplotlib.pyplot as plt
import sys
import pandas
import numpy as np
import math
from os import listdir
import os


if __name__ == "__main__":
    print("Markus Super Duper Motivation Plotter")

    root_directory = "."
    if len(sys.argv) > 1:
        root_directory = sys.argv[1]

    if len(sys.argv) > 2:
        file_names = sys.argv[2]
    else:
        file_names = [filename for filename in listdir(root_directory) if filename.endswith(".csv")]

    best_ranks = []
    best_rank_duration_ratios = []

    for file_name in file_names:
        try:
            df = pandas.read_csv(os.path.join(root_directory, file_name), sep=",")
        except Exception as e:
            print("Couldn't parse {}: {}".format(file_name, e))
            continue

        durations = df["Duration"]

        if len(durations) == 0 or durations[0] == 0:
            print("Not considering {}, it has timeout for its first plan or no measurements".format(file_name))
            continue

        base_duration = durations[0]
        best_duration = base_duration
        best_idx = 0

        for idx, duration in durations.iteritems():
            if duration == 0:
                continue
            if duration < best_duration:
                best_idx = idx
                best_duration = duration

        ratio = float(base_duration) / best_duration

        best_ranks.append(min(best_idx, 300))
        best_rank_duration_ratios.append(min(2.5, ratio))
        print("{}: BestPlan: {}, Ratio: {}".format(file_name, best_idx, ratio))

    plt.plot(best_ranks, best_rank_duration_ratios, "bo")
    plt.grid(True)
    plt.savefig("{}.svg".format("motivation_plot"), format="svg")

