cat scripts/supported_job_queries.txt | parallel -j $3 --eta --results $4 $1 $2 {}
