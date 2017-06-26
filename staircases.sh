#!/bin/bash
set -e

sleep_time=$((60 - $(date +%S)))
echo "Sleeping ${sleep_time} secs to start exactly at $(date --date +${sleep_time}sec +%FT%H:%M:%S)"
sleep ${sleep_time}

exe="/home/ec2-user/range_random_query/range_random_query"
conn_uri="mongodb://akira:secret@ip-172-31-38-225:27017/"

function pummel {
  run_interval=$1
  min_id=$2
  max_id=$3
  echo "$(date +%FT%H:%M:%S): Querying range ${min_id} to ${max_id}"
  ${exe} -m ${conn_uri} -d ptp_as_demo -c foo -t 16 -i ${run_interval} -x ${min_id} -y ${max_id}
}

#pummel 600        1 15000000
x=18000001
y=20000000
pummel 600 ${x} ${y} #warmup original
exit
#while [ $((y - x)) -lt 20000000 ]; do #20M docs
while [ $((y - x)) -lt  10000000 ]; do #6M docs
  pummel 60 ${x} ${y}
  y=$((y + 500000))
done

#I forget what test I used the following ranges in
#y=40000000
#while [ $((y - x)) -lt 50000000 ]; do
#  pummel 60 ${x} ${y}
#  y=$((y + 4000000))
#done
