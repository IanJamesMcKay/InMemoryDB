cat scripts/supported_tpch_queries.txt | parallel -j $3 --eta --results $4 $1 $2 {}
