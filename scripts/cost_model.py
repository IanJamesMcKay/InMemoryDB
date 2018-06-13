import json
import pandas
import sklearn.linear_model
import matplotlib.pyplot as plt

samples_path = "/home/moritz/pella/hyrise/joe/cost-sample/CostFeatureSamples-Release.json"

with open(samples_path) as file:
    samples_json = json.load(file)

for operator, operator_samples_json in samples_json.items():
    if operator != "JoinHash":
        continue

    print("{}: {} samples".format(operator, len(operator_samples_json)))

    df = pandas.DataFrame(operator_samples_json)
    df = df[df.Runtime < 300*1000*1000]
    df = df[df.Runtime > 1000]

    y = df["Runtime"]


    X = pandas.DataFrame(df, columns=["MajorInputRowCount", "MinorInputRowCount", "OutputRowCount"])


    reg = sklearn.linear_model.Lars(fit_intercept=False, positive=True)
    model = reg.fit(X, y)

    predictions = model.predict(X)


    for idx, y_pred in enumerate(predictions):
        print(y.iloc[idx], int(y_pred))

    print(model.score(X,y))
    print(model.coef_)

    #reg = sklearn.linear_model.LinearRegression(fit_intercept=False)
    #model = reg.fit(X, y)

   # ax = plt.gca()
   # ax.set_yscale('log')
   # ax.set_xscale('log')

    plt.scatter(y, predictions, s=0.4)
    plt.show()


