SCALE=0.5
./cmake-build-release/hyriseBenchmarkTPCH --mvcc --scale $SCALE -o result_wo_pc_wo_rl.json
./cmake-build-release/hyriseBenchmarkTPCH --mvcc --punch_card --scale $SCALE -o result_w_pc_wo_rl.json
./cmake-build-release/hyriseBenchmarkTPCH --mvcc --rate_limiting --scale $SCALE -o result_wo_pc_w_rl.json
./cmake-build-release/hyriseBenchmarkTPCH --mvcc --punch_card --rate_limiting --scale $SCALE -o result_w_pc_w_rl.json
