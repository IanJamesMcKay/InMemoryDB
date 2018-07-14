import os

import subprocess
from subprocess import run

CWD = '/home/Leander.Neiss/hyrise-masterthesis/'
# RESULT_DIR = 'tpch-scale0.1/'
EXECUTABLE = 'build-release/numaJOB'
STATUS_FILE = 'current_run.status'
OUTPUT_FILE = 'current_run.out'
NAME_FILE = 'current_run.name'

USE_SCHEDULER = 'true'

WORKLOAD = 'tpch'
SCALE = 0.1
ITERATIONS_PER_QUERY = 100
ITERATIONS_OVERALL = 5
# ITERATIONS_PER_QUERY = 1
# ITERATIONS_OVERALL = 1

RESULT_DIR = WORKLOAD + '_scale' + str(SCALE) + '_iterations' + str(ITERATIONS_PER_QUERY) + '_take2'

# core_counts = ['80', '70', '60', '50', '40', '30', '20', '10', '5', '2', '1']
# core_counts = ['80', '40', '0']
# core_counts = ['80']
# core_counts = ['0']
# core_counts = [str(cores) for cores in range(80, 0, -1)]
# core_counts = [str(cores) for cores in range(80, -1, -5)]

core_counts = [str(cores) for cores in range(80, 1, -2)] + ['0']
# core_counts = [str(cores) for cores in range(79, 2, -2)]

if os.path.exists(STATUS_FILE):
    os.remove(STATUS_FILE)
if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)
if os.path.exists(NAME_FILE):
    os.remove(NAME_FILE)

for iteration in range(ITERATIONS_OVERALL):
    for core_count in core_counts:
        with open(STATUS_FILE, 'a+') as status:
            status.write('Running iteration ' + str(iteration) + ', core count ' + str(core_count) + '...\n')
        with open(OUTPUT_FILE, 'a+') as output:
            run([
                EXECUTABLE,
                '-w', WORKLOAD,
                '-e', RESULT_DIR,
                '-s', str(SCALE),
                '--iterations-per-query', str(ITERATIONS_PER_QUERY),
                '--use-scheduler=true',
                '--numa-cores', '' + core_count,
                '--iteration', str(iteration)
                ], cwd=CWD, stdout=output, stderr=output)

with open(STATUS_FILE, 'a+') as status:
    status.write('Done\n')
with open(NAME_FILE, 'w') as name_file:
    name_file.write(RESULT_DIR + '\n')
