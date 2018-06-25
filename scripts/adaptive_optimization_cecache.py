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

if __name__ == "__main__":
    # plt.figure(figsize=(25, 13))

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

    valid_measurement_count = 0

    hit_frequencies_matrix = []

    for name, df in data_frames:
        cecache_distinct_hit_counts = df["CECacheDistinctHitCount"]
        cecache_distinct_miss_counts = df["CECacheDistinctMissCount"]
        cecache_distinct_access_counts = np.add(cecache_distinct_hit_counts, cecache_distinct_miss_counts)

        hit_frequencies = np.divide(cecache_distinct_hit_counts, cecache_distinct_access_counts)
        hit_frequencies_matrix.append(hit_frequencies)

        hit_frequencies_plot = hit_frequencies.copy()

        convergence_marker = (iteration_count - 1, hit_frequencies_plot[iteration_count - 1])

        for iteration_idx in range(iteration_count - 2, 0, -1):
            if hit_frequencies_plot[iteration_idx + 1] == hit_frequencies_plot[iteration_idx]:
                hit_frequencies_plot[iteration_idx + 1] = None
                convergence_marker = (iteration_idx, hit_frequencies_plot[iteration_idx])
            else:
                break

        lines = plt.plot(hit_frequencies_plot.tolist(), linewidth=0.5, label=None)
        color = lines[0].get_color()

        plt.scatter(x=convergence_marker[0], y=convergence_marker[1], zorder=1, s=2, color=color)

        #assert len(np.unique(cecache_distinct_access_counts)) == 1

    hit_frequencies_matrix_df = pandas.DataFrame(hit_frequencies_matrix)
    hit_frequencies_matrix_df_mean = hit_frequencies_matrix_df.mean().tolist()

    mean_lines = plt.plot(hit_frequencies_matrix_df_mean, label="Average", linewidth=1.4)
    mean_color = mean_lines[0].get_color()

    mean_annotation = "{:.2f}".format(hit_frequencies_matrix_df_mean[-1])
    plt.annotate(mean_annotation, xy=(iteration_count - 1, hit_frequencies_matrix_df_mean[-1]), color=mean_color, fontsize=9, zorder=2)

    plt.xlim(xmin=0, xmax=iteration_count)
    plt.ylim(ymin=0, ymax=1)
    plt.xticks(range(0, iteration_count))
    plt.xlabel("Iteration index")
    plt.ylabel("CardinalityEstimationCache hit frequency")
    plt.legend()
    plt.savefig("adaptive_optimization_cecache.png")
    plt.show()