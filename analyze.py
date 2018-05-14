import json
import sys
from pprint import pprint

folder = sys.argv[1] if len(sys.argv) > 1 else '.';

with open(folder + '/result_wo_pc_wo_rl.json') as wo_pc_wo_rl, \
    open(folder + '/result_wo_pc_w_rl.json') as wo_pc_w_rl, \
    open(folder + '/result_w_pc_wo_rl.json') as w_pc_wo_rl, \
    open(folder + '/result_w_pc_w_rl.json') as w_pc_w_rl:
    data = []
    data.append(json.load(wo_pc_wo_rl))
    data.append(json.load(w_pc_wo_rl))
    data.append(json.load(wo_pc_w_rl))
    data.append(json.load(w_pc_w_rl))
    print(['wo_pc_wo_rl', 'w_pc_wo_rl', 'wo_pc_w_rl', 'w_pc_w_rl'])
    for measurement_type in ['cpu_time', 'real_time', 'items_per_second']:
        print(measurement_type + ':')
        avg = [0, 0, 0, 0]
        avg_abs = [0, 0, 0, 0]
        for i in range(len(data[0]['benchmarks'])):
            print(data[0]['benchmarks'][i]['name'], ': ', end='')
            for benchmark in data:
                print(round(benchmark['benchmarks'][i][measurement_type], 4), end=', ')
            print('')
            print(' ' * (len(data[0]['benchmarks'][i]['name'])-2), '% : ', end='')
            for j in range(len(data)):
                benchmark = data[j]
                dev = benchmark['benchmarks'][i][measurement_type]/data[0]['benchmarks'][i][measurement_type]
                avg[j] += 1 - dev
                avg_abs[j] += abs(1 - dev)
                print(round(dev * 100, 2), end=', ')
            print('')
        print('average deviation (%): ', end='')
        for average in avg:
            print(round(average / len(data[0]['benchmarks']) * 100, 4), ', ', end='')
        print('')
        print('average abs deviation (%): ', end='')
        for average in avg_abs:
            print(round(average / len(data[0]['benchmarks']) * 100, 4), ', ', end='')
        print('')
    #pprint(data)