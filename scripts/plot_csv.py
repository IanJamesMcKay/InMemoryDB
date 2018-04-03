import matplotlib.pyplot as plt
import sys
import pandas
import numpy as np
import math
from os import listdir

timeout_threshold = 1000 * 1000 * 60 * 60


def fix_gaps(row):
    if row["Duration"] >= timeout_threshold or row["Duration"] == 0:
        row[:] = float("nan")
    return row


def merge_list(left,right):
    result = list()
    i,j = 0,0
    inv_count = 0
    while i < len(left) and j < len(right):
        if left[i] < right[j]:
            result.append(left[i])
            i += 1
        elif right[j] < left[i]:
            result.append(right[j])
            j += 1
            inv_count += (len(left)-i)
        else:
            result.append(left[i])
            result.append(right[j])
            i += 1
            j += 1

    result += left[i:]
    result += right[j:]
    return result,inv_count


def sort_and_count(array):
    if len(array) < 2:
        return array, 0
    middle = len(array) / 2
    left,inv_left = sort_and_count(array[:middle])
    right,inv_right = sort_and_count(array[middle:])
    merged, count = merge_list(left,right)
    count += (inv_left + inv_right)
    return merged, count

def num_pairs(n):
    if n <= 1:
        return 0
    if n == 2:
        return 1
    return (n - 1) + num_pairs(n - 1)

if __name__ == "__main__":
    print("Plot CSVs")

    if len(sys.argv) != 2:
        file_names = [filename for filename in listdir(".") if filename.endswith(".csv")]
    else:
        file_names = sys.argv[1]

    #file_names = ["debug-TPCH-5-CostModelNaive-sf0.010000.csv"]

    for file_name in file_names:

        try:
            df = pandas.read_csv(file_name, sep=",")
        except Exception as e:
            print("Couldn't parse {}: {}".format(file_name, e))
            continue

        durations = df["Duration"].tolist()
        _, inversions = sort_and_count(durations)
        print("{}/{} inversions in {}".format(inversions, num_pairs(len(durations)), file_name))

        if df.shape[0] > 1:
            df = df.apply(fix_gaps, axis=1, reduce=False)

        last_valid_row_idx = 0
        for idx, row in df.iterrows():
            if not math.isnan(row["Duration"]):
                last_valid_row_idx = idx
        df = df.iloc[:last_valid_row_idx + 1,:]

        for y_idx in range(1, df.shape[1]):
            plt.plot(df.iloc[:,y_idx], linewidth=0.6, label=df.columns[y_idx])


        plt.legend()
        plt.xlabel("rank")
        plt.ylabel("cost")
        plt.savefig("{}.svg".format(file_name), format="svg")
        plt.close()
