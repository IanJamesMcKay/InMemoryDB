cat scripts/supported_job_queries.txt | parallel -j 8 --eta --results joe_parallel_output $1 $2 {}
