from os import listdir
import os
import sys
import pandas
import matplotlib.pyplot as plt



if __name__ == "__main__":
    root_directory = "."
    if len(sys.argv) > 1:
        root_directory = sys.argv[1]

    file_names = [filename for filename in listdir(root_directory) if filename.endswith(".csv")]

    accumulated_normalized_durations = []
    valid_measurement_count = 0

    for file_name in file_names:
        print("Processing {}".format(file_name))
        try:
            df = pandas.read_csv(os.path.join(root_directory, file_name), sep=",")
        except Exception as e:
            print("Couldn't parse {}: {}".format(file_name, e))
            continue

        if "Duration" not in df:
            print("Skipping")
            continue

        durations = df["Duration"]

        # Init accumulated_normalized_durations
        if len(accumulated_normalized_durations) == 0:
            accumulated_normalized_durations = [0]*len(durations)
        else:
            assert len(accumulated_normalized_durations) == len(durations)

        if durations[0] == 0:
            print("Skipping")
            continue
uu
        normalized_durations = []

        for idx, duration in enumerate(durations):
            normalized_duration = min(2, duration / durations[0])
            normalized_durations.append(normalized_duration)
            accumulated_normalized_durations[idx] += normalized_duration

        plt.plot(normalized_durations, linewidth=0.2)
        valid_measurement_count += 1

    accumulated_normalized_durations = [d / valid_measurement_count for d in accumulated_normalized_durations]

    plt.plot(accumulated_normalized_durations, label="Average")

    plt.xlabel("Query Iteration")
    plt.ylabel("Performance relative to first iteration")
    plt.legend()
    plt.axhline(y=1)
    plt.savefig("{}{}.svg".format("query_iterations-", os.path.split(os.path.split(root_directory)[0])[1]), format="svg")

