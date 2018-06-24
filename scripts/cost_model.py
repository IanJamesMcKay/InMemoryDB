import json
import pandas
import sklearn.linear_model
import sklearn.metrics
import numpy as np
import matplotlib.pyplot as plt

def mean_absolute_percentage_error(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    abs_errors = np.abs((y_true - y_pred) / y_true)
    return np.mean(abs_errors) * 100


samples_path = "/home/moritz/CostFeatureSamples-Release.json"

features_by_operator = {
    "JoinHash": ["MajorInputRowCount", "MinorInputRowCount", "OutputRowCount"],
    "TableScan_Int": ["LeftInputRowCount", "LeftInputReferenceRowCount", "OutputRowCount"],
    "TableScan_Long": ["LeftInputRowCount", "LeftInputReferenceRowCount", "OutputRowCount"],
    "TableScan_Like": ["LeftInputRowCount", "LeftInputReferenceRowCount", "OutputRowCount"],
}


with open(samples_path) as file:
    samples_json = json.load(file)

samples = {}
samples["TableScan_Int"] = []
samples["TableScan_Long"] = []
samples["TableScan_Like"] = []

for key, operator_samples in samples_json.items():
    if key == "TableScan":
        for operator_sample in operator_samples:
            if operator_sample["RightDataType"] == "int":
                samples["TableScan_Int"].append(operator_sample)
            elif operator_sample["RightDataType"] == "long":
                samples["TableScan_Long"].append(operator_sample)
            elif operator_sample["PredicateCondition"] == "LIKE":
                samples["TableScan_Like"].append(operator_sample)

    else:
        samples[key] = operator_samples

for operator, operator_samples in samples.items():
    if operator not in features_by_operator:
        continue
    if len(operator_samples) == 0:
        continue

    print("{}: {} samples".format(operator, len(operator_samples)))

    for sample in operator_samples:
        if sample["LeftInputRowCount"] != sample["OutputRowCount"]:
            print(sample)

    df = pandas.DataFrame(operator_samples)
    i = pandas.DataFrame(df, columns=["LeftInputRowCount", "OutputRowCount", "Runtime"])
    df = df[df.Runtime < 300*1000*1000]
    df = df[df.Runtime > 1000]

    if df.shape[0] == 0:
        continue

    target = "Runtime"
    features = features_by_operator[operator]

    df = pandas.DataFrame(df, columns=features + [target])

    y = df["Runtime"]
    X = pandas.DataFrame(df, columns=features)

    reg = sklearn.linear_model.Lars(fit_intercept=False, positive=True)
    model = reg.fit(X, y)

    y_preds = model.predict(X)

    #for idx, y_pred in enumerate(y_preds):
    #    print(y.iloc[idx], int(y_pred))

    print("  Coef: {}".format(model.coef_))

    #reg = sklearn.linear_model.LinearRegression(fit_intercept=False)
    #model = reg.fit(X, y)

   # ax = plt.gca()
   # ax.set_yscale('log')
   # ax.set_xscale('log')

    print("  MAPE: {}".format(mean_absolute_percentage_error(y, y_preds)))
    print("  R2: {}".format(sklearn.metrics.r2_score(y, y_preds)))

    plt.title(operator)
    plt.scatter(y, y_preds, s=0.4)
    plt.show()


