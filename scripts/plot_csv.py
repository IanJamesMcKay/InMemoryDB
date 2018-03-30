import matplotlib.pyplot as plt
import sys
import pandas
from os import listdir


if __name__ == "__main__":
    if len(sys.argv) != 2:
        file_names = [filename for filename in listdir(".") if filename.endswith(".csv")]
    else:
        file_names = sys.argv[1]

    for file_name in file_names:
        try:
            df = pandas.read_csv(file_name, sep=",")
        except Exception:
            print("Couldn't parse {}".format(file_name))
            continue

        for y_idx in range(1, df.shape[1]):
            plt.semilogy(df.iloc[:,0], df.iloc[:,y_idx], linewidth=0.6, label=df.columns[y_idx])

        plt.legend()
        plt.xlabel("rank")
        plt.ylabel("cost")
        plt.savefig("{}.svg".format(file_name), format="svg")
        plt.close()
