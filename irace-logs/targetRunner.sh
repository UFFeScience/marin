#!/bin/bash
# local: /home/italo/R/x86_64-pc-linux-gnu-library/4.4/irace/bin/irace >> irace.log &
# m3labesi: /home/italons/R/x86_64-pc-linux-gnu-library/4.1/irace/bin/irace >> irace.log &
nohup python3 /home/italo/Projects/MESTRADO/estudo-orientado-2024-2/irace-bruna/irace-spark/irace-logs/main.py --configId $1 --instanceId $2 --seed $3 --instance $4 ${@:5}>> logs/python_log.log 2>&1  </dev/null & 

process_id=$!

wait $process_id

tag=$( tail -n 1  logs/python_log.log )

echo $tag
