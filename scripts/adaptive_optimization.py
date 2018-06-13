from os import listdir
import os
import sys
import pandas
import matplotlib.pyplot as plt
import re
import numpy as np
import interesting_job_queries

reference_duration_type = "baseline"
use_whitelist = True
root_directory = "/home/moritz/pella/hyrise/joe/penalty"

# normalized duration to assume if query timed out
timeout_normalized_duration = 10

if __name__ == "__main__":
    plt.figure(figsize=(25, 13))
    
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

    if reference_duration_type == "baseline":
        baseline_df = pandas.read_csv("baseline.csv", sep=",", index_col=0)

    print("Iteration Count: {}".format(iteration_count))

    normalized_duration_matrix = [[] for idx in range(iteration_count)]
    valid_measurement_count = 0

    for name, df in data_frames:
        durations = df["RankZeroPlanExecutionDuration"]
        hashes = df["RankZeroPlanHash"]

        if reference_duration_type == "first_iteration":
            reference_duration = next((duration for duration in durations if duration > 0), 0)
        else:
            reference_duration = baseline_df["MinExeDuration"][name]

        if reference_duration == 0:
            print("Skipping {}".format(name))
            continue

        indices = []
        normalized_durations = []
        last_hash = hashes[iteration_count - 1]
        fastest_baseline_warning = 1.0
        convergence_marker = None
        plot = True

        #if hashes[0] == hashes[iteration_count - 1] and durations[0] > 0:
        #    print("Skipping {}, plan converged to early".format(name))
        #    continue

        for idx, (duration, hash) in enumerate(zip(durations, hashes)):
            normalized_duration = duration / reference_duration

            if plot:
                if duration == 0:
                    normalized_durations.append(None)
                else:
                    normalized_durations.append(normalized_duration)

                if normalized_duration < fastest_baseline_warning and reference_duration_type == "baseline" and normalized_duration >0:
                    print("{} faster than baseline?!? ({})".format(name, normalized_duration))
                    fastest_baseline_warning = normalized_duration

                indices.append(idx)

            if normalized_duration > 0:
                normalized_duration_matrix[idx].append(normalized_duration)
            else:
                normalized_duration_matrix[idx].append(timeout_normalized_duration)


            if hash == last_hash and plot:
                print("{} converged at {} @ {}".format(name, normalized_duration, idx))
                if normalized_duration != 0:
                    convergence_marker = (idx, normalized_duration)
                plot = False

        lines = plt.plot(indices, normalized_durations, linewidth=0.4, zorder=0)
        color = lines[0].get_color()
        if convergence_marker is not None:
            plt.scatter(x=convergence_marker[0], y=convergence_marker[1], zorder=1, s=2, color=color)
            plt.annotate(name, xy = convergence_marker, fontsize=5, color=color)
        valid_measurement_count += 1

    average = [np.average(normalized_durations) for normalized_durations in normalized_duration_matrix]
    #median = [np.median(normalized_durations) for normalized_durations in normalized_duration_matrix]

    plt.plot(average, label="Average", linewidth=1.5)
    #plt.plot(median, label="Median", linewidth=0.9)

    plt.xlabel("Query Iteration")
    plt.ylabel("Performance relative to {}".format(reference_duration_type))
    plt.legend()
    plt.axhline(y=1, linewidth=0.5)
    plt.ylim(ymin=0.8, ymax=10)
    #plt.show()
    plt.savefig("iterations-relative-to-{}-{}.svg".format(reference_duration_type, evaluation_name), format="svg", dpi=900)

