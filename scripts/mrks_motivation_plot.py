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
    better_ranks = []
    better_rank_duration_ratios = []

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

        xlim=5000
        ylim=2.5

        for idx, duration in durations.iteritems():
            if duration == 0:
                continue
            if duration < best_duration:
                best_idx = idx
                best_duration = duration
            elif duration < base_duration:
                better_ranks.append(idx)
                ratio = float(base_duration) / duration
                #ratio = min(ylim, ratio)
                better_rank_duration_ratios.append(ratio)


        ratio = float(base_duration) / best_duration

        best_idx=min(best_idx, xlim)
        best_ranks.append(best_idx)
        ratio =min(ylim, ratio)
        best_rank_duration_ratios.append(ratio)
        print("{}: BestPlan: {}, Ratio: {}".format(file_name, best_idx, ratio))

    plt.plot(best_ranks, best_rank_duration_ratios, "bo", markersize=4.5, label="Best Plan Found")
    plt.legend()
    plt.xlabel("rank")
    plt.ylabel("relative performance to rank #0")
    #plt.plot(better_ranks, better_rank_duration_ratios, "o", markersize=0.1)
    plt.grid(True)
    plt.savefig("{}.svg".format("where_is_the_best_plan"), format="svg")

