#!/bin/bash

# Logging variables
log_file=/starvers_eval/output/logs/download/downloads.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

# Prepare directories and files
rm -rf /starvers_eval/output/logs/download
mkdir -p /starvers_eval/output/logs/download
> $log_file

# Path variables
snapshot_dir=`grep -A 2 '[General]' /starvers_eval/configs/eval_setup.toml | awk -F '"' '/snapshot_dir/ {print $2}'`

# Download
echo "$(log_timestamp) ${log_level}: Downloading BEARA datasets..." >> $log_file
wget -t 3 -c -P /starvers_eval/rawdata/beara https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/datasets/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers_eval/rawdata/beara/${snapshot_dir}
tar -xf /starvers_eval/rawdata/beara/alldata.IC.nt.tar.gz -C /starvers_eval/rawdata/beara/${snapshot_dir}
wget -t 3 -c -P /starvers_eval/rawdata/beara https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/datasets/TB/alldata.TB.nq.gz
gzip -d < /starvers_eval/rawdata/beara/alldata.TB.nq.gz > /starvers_eval/rawdata/beara/alldata.TB.nq
> /starvers_eval/rawdata/beara/empty.nt # for CB and CBNG policy: empty initial delete changeset
echo "$(log_timestamp) ${log_level}: Downloading BEARA datasets finished." >> $log_file

echo "$(log_timestamp) ${log_level}: Downloading BEARB-hour datasets..." >> $log_file
wget -t 3 -c -P /starvers_eval/rawdata/bearb_hour https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/hour/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers_eval/rawdata/bearb_hour/${snapshot_dir}
tar -xf /starvers_eval/rawdata/bearb_hour/alldata.IC.nt.tar.gz -C /starvers_eval/rawdata/bearb_hour/${snapshot_dir}
cd /starvers_eval/rawdata/bearb_hour/${snapshot_dir}
for f in *.gz ; do gzip -d < "$f" > /starvers_eval/rawdata/bearb_hour/${snapshot_dir}/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P /starvers_eval/rawdata/bearb_hour https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/hour/TB/alldata.TB.nq.gz
gzip -d < /starvers_eval/rawdata/bearb_hour/alldata.TB.nq.gz > /starvers_eval/rawdata/bearb_hour/alldata.TB.nq
> /starvers_eval/rawdata/bearb_hour/empty.nt # for CB and CBNG policy: empty initial delete changeset
echo "$(log_timestamp) ${log_level}: Downloading BEARB-hour datasets finished." >> $log_file

echo "$(log_timestamp) ${log_level}: Downloading BEARB-day datasets..." >> $log_file
wget -t 3 -c -P /starvers_eval/rawdata/bearb_day https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/day/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers_eval/rawdata/bearb_day/${snapshot_dir}
tar -xf /starvers_eval/rawdata/bearb_day/alldata.IC.nt.tar.gz -C /starvers_eval/rawdata/bearb_day/${snapshot_dir}
cd /starvers_eval/rawdata/bearb_day/${snapshot_dir}
for f in *.gz ; do gzip -d < "$f" > /starvers_eval/rawdata/bearb_day/${snapshot_dir}/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P /starvers_eval/rawdata/bearb_day https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/day/TB/alldata.TB.nq.gz
gzip -d < /starvers_eval/rawdata/bearb_day/alldata.TB.nq.gz > /starvers_eval/rawdata/bearb_day/alldata.TB.nq
> /starvers_eval/rawdata/bearb_day/empty.nt # for CB and CBNG policy: empty initial delete changeset
echo "$(log_timestamp) ${log_level}: Downloading BEARB-day datasets finished." >> $log_file

echo "$(log_timestamp) ${log_level}: Downloading BEARC datasets..." >> $log_file
wget -t 3 -c -P /starvers_eval/rawdata/bearc https://aic.ai.wu.ac.at/qadlod/bear/BEAR_C/datasets/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers_eval/rawdata/bearc/${snapshot_dir}
tar -xf /starvers_eval/rawdata/bearc/alldata.IC.nt.tar.gz -C /starvers_eval/rawdata/bearc/${snapshot_dir}
cd /starvers_eval/rawdata/bearc/${snapshot_dir}
for f in *.gz ; do gzip -d < "$f" > /starvers_eval/rawdata/bearc/${snapshot_dir}/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P /starvers_eval/rawdata/bearc https://aic.ai.wu.ac.at/qadlod/bear/BEAR_C/datasets/TB/alldata.TB.nq.gz
gzip -d < /starvers_eval/rawdata/bearc/alldata.TB.nq.gz > /starvers_eval/rawdata/bearc/alldata.TB.nq
> /starvers_eval/rawdata/bearc/empty.nt # for CB and CBNG policy: empty initial delete changeset
echo "$(log_timestamp) ${log_level}: Downloading BEARC datasets finished." >> $log_file
