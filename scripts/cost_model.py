import json


samples_path = "/home/moritz/pella/hyrise/joe/cost-sample/CostFeatureSamples-Release.json"

with open(samples_path) as file:
    samples_json = json.load(file)

for operator, operator_samples_json in samples_json:

