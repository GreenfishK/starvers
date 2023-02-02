#!/bin/bash

# Logging variables
log_file=/starvers_eval/output/logs/download/downloads.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

# Prepare directories and files
rm -rf /starvers_eval/output/logs/download
mkdir -p /starvers_eval/output/logs/download
> $log_file


# Download
echo "$(log_timestamp) ${log_level}: Downloading BEARA datasets..." >> $log_file
wget -t 3 -c -P /starvers_eval/rawdata/beara https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/datasets/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers_eval/rawdata/beara/alldata.IC.nt
tar -xf /starvers_eval/rawdata/beara/alldata.IC.nt.tar.gz -C /starvers_eval/rawdata/beara/alldata.IC.nt
wget -t 3 -c -P /starvers_eval/rawdata/beara https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/datasets/TB/alldata.TB.nq.gz
gzip -d < /starvers_eval/rawdata/beara/alldata.TB.nq.gz > /starvers_eval/rawdata/beara/alldata.TB.nq
> /starvers_eval/rawdata/beara/empty.nt # for CB and CBNG policy: empty initial delete changeset
echo "$(log_timestamp) ${log_level}: Downloading BEARA datasets finished." >> $log_file

echo "$(log_timestamp) ${log_level}: Downloading BEARB-hour datasets..." >> $log_file
wget -t 3 -c -P /starvers_eval/rawdata/bearb_hour https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/hour/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers_eval/rawdata/bearb_hour/alldata.IC.nt
tar -xf /starvers_eval/rawdata/bearb_hour/alldata.IC.nt.tar.gz -C /starvers_eval/rawdata/bearb_hour/alldata.IC.nt
cd /starvers_eval/rawdata/bearb_hour/alldata.IC.nt
for f in *.gz ; do gzip -d < "$f" > /starvers_eval/rawdata/bearb_hour/alldata.IC.nt/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P /starvers_eval/rawdata/bearb_hour https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/hour/TB/alldata.TB.nq.gz
gzip -d < /starvers_eval/rawdata/bearb_hour/alldata.TB.nq.gz > /starvers_eval/rawdata/bearb_hour/alldata.TB.nq
> /starvers_eval/rawdata/bearb_hour/empty.nt # for CB and CBNG policy: empty initial delete changeset
echo "$(log_timestamp) ${log_level}: Downloading BEARB-hour datasets finished." >> $log_file

echo "$(log_timestamp) ${log_level}: Downloading BEARB-day datasets..." >> $log_file
wget -t 3 -c -P /starvers_eval/rawdata/bearb_day https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/day/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers_eval/rawdata/bearb_day/alldata.IC.nt
tar -xf /starvers_eval/rawdata/bearb_day/alldata.IC.nt.tar.gz -C /starvers_eval/rawdata/bearb_day/alldata.IC.nt
cd /starvers_eval/rawdata/bearb_day/alldata.IC.nt
for f in *.gz ; do gzip -d < "$f" > /starvers_eval/rawdata/bearb_day/alldata.IC.nt/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P /starvers_eval/rawdata/bearb_day https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/day/TB/alldata.TB.nq.gz
gzip -d < /starvers_eval/rawdata/bearb_day/alldata.TB.nq.gz > /starvers_eval/rawdata/bearb_day/alldata.TB.nq
> /starvers_eval/rawdata/bearb_day/empty.nt # for CB and CBNG policy: empty initial delete changeset
echo "$(log_timestamp) ${log_level}: Downloading BEARB-day datasets finished." >> $log_file

echo "$(log_timestamp) ${log_level}: Downloading BEARC datasets..." >> $log_file
wget -t 3 -c -P /starvers_eval/rawdata/bearc https://aic.ai.wu.ac.at/qadlod/bear/BEAR_C/datasets/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers_eval/rawdata/bearc/alldata.IC.nt
tar -xf /starvers_eval/rawdata/bearc/alldata.IC.nt.tar.gz -C /starvers_eval/rawdata/bearc/alldata.IC.nt
cd /starvers_eval/rawdata/bearc/alldata.IC.nt
for f in *.gz ; do gzip -d < "$f" > /starvers_eval/rawdata/bearc/alldata.IC.nt/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P /starvers_eval/rawdata/bearc https://aic.ai.wu.ac.at/qadlod/bear/BEAR_C/datasets/TB/alldata.TB.nq.gz
gzip -d < /starvers_eval/rawdata/bearc/alldata.TB.nq.gz > /starvers_eval/rawdata/bearc/alldata.TB.nq
> /starvers_eval/rawdata/bearc/empty.nt # for CB and CBNG policy: empty initial delete changeset
echo "$(log_timestamp) ${log_level}: Downloading BEARC datasets finished." >> $log_file
