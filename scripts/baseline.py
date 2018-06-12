from os import listdir
import os
import sys
import matplotlib.pyplot as plt
import numpy
import csv
import re


if __name__ == "__main__":
    root_directory = "."
    if len(sys.argv) > 1:
        root_directory = sys.argv[1]

    file_names = [file_name for file_name in listdir(root_directory) if file_name.endswith(".csv")]

    result = []

    for file_name in file_names:
        print("Processing {}".format(file_name))

        file_path = os.path.join(root_directory, file_name)
        c = numpy.genfromtxt(file_path, names=True, delimiter=",")

        if "RankZeroPlanExecutionDuration" not in c.dtype.fields:
            print("  Skipping")
            continue

        job_match = re.match(".*JOB-([0-9]+[a-z]+)", file_name)
        if job_match is None:
            print("  Skipping")
            continue

        name = job_match.group(1)

        durations = c["RankZeroPlanExecutionDuration"]

        print("  {}: {}".format(name, numpy.min(durations)))

        result.append((name, numpy.min(durations), numpy.max(durations), numpy.average(durations), numpy.median(durations)))

    with open("baseline.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(['Name', "MinExeDuration", "MaxExeDuration", "AverageExeDuration", "MedianExeDuration"])
        writer.writerows(result)