import matplotlib.pyplot as plt
import sys
import pandas
from os import listdir


if __name__ == "__main__":
    print("Plot CSVs")

    if len(sys.argv) != 2:
        file_names = [filename for filename in listdir(".") if filename.endswith(".csv")]
    else:
        file_names = sys.argv[1]

    for file_name in file_names:
        print("Processing {}".format(file_name))

        try:
            df = pandas.read_csv(file_name, sep=",")
        except Exception:
            print("Couldn't parse {}".format(file_name))
            continue

        df = df.iloc[:, 1:df.shape[1]]
        df = df[(df.T != 0).any()]

        for y_idx in range(0, df.shape[1]):
            plt.semilogy(df.iloc[:,y_idx].as_matrix(), linewidth=0.6, label=df.columns[y_idx])

        plt.legend()
        plt.xlabel("rank")
        plt.ylabel("cost")
        plt.savefig("{}.svg".format(file_name), format="svg")
        plt.close()
