#!/bin/sh

SESSION=moautosession
COMMAND="./cmake-build-clang-release/hyriseJoinOrderingEvaluator -w job --timeout-plan 320 --timeout-query 7200 --max-plan-count 1500 --visualize"

echo $COMMAND

tmux -2 new-session -d -s $SESSION

# Setup a window for tailing log files
tmux new-window -t $SESSION:1 -n 'Evaluators'
tmux select-layout tiled

tmux split-window -h

tmux select-pane -t 0

tmux send-keys "numactl -N 0 -- $COMMAND -- 1a 1b 1c 1d 2a 2b 2c 2d 4a &> batch0.log" C-m
tmux split-window -v
tmux send-keys "numactl -N 0 -- $COMMAND -- 4b 4c 6a 6c 6e 7a 7b &> batch1.log" C-m
tmux split-window -v
tmux send-keys "numactl -N 1 -- $COMMAND -- 8a 8b 8c 8d 9b 10a 10b &> batch2.log" C-m
tmux split-window -v
tmux send-keys "numactl -N 1 -- $COMMAND -- 10c 11a 11b 13a 13b &> batch3.log" C-m

tmux select-pane -t 4
tmux send-keys "numactl -N 2 -- $COMMAND -- 13c 13d 15a 15b 15c 15d &> batch4.log" C-m
tmux split-window -v
tmux send-keys "numactl -N 2 -- $COMMAND -- 16a 16b 16c 16d 17a 17b &> batch5.log" C-m
tmux split-window -v
tmux send-keys "numactl -N 3 -- $COMMAND -- 17c 17d 17e 17f &> batch6.log" C-m
tmux split-window -v
tmux send-keys "numactl -N 3 -- $COMMAND -- 19b 32a 32b 33a &> batch7.log" C-m

# Set default window
tmux select-window -t $SESSION:1

# Attach to session
tmux -2 attach-session -t $SESSION

