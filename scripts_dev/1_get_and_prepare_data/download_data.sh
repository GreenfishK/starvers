#!/bin/bash
wget -t 3 -c -P /starvers_eval/rawdata/beara https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/datasets/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers_eval/rawdata/beara/alldata.IC.nt
tar -xf /starvers_eval/rawdata/beara/alldata.IC.nt.tar.gz -C /starvers_eval/rawdata/beara/alldata.IC.nt
wget -t 3 -c -P /starvers_eval/rawdata/beara https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/datasets/TB/alldata.TB.nq.gz
gzip -d < /starvers_eval/rawdata/beara/alldata.TB.nq.gz > /starvers_eval/rawdata/beara/alldata.TB.nq
> /starvers_eval/rawdata/beara/empty.nt # for CB policy: empty initial delete changeset

wget -t 3 -c -P /starvers_eval/rawdata/bearb-hour https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/hour/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers_eval/rawdata/bearb-hour/alldata.IC.nt
tar -xf /starvers_eval/rawdata/bearb-hour/alldata.IC.nt.tar.gz -C /starvers_eval/rawdata/bearb-hour/alldata.IC.nt
cd /starvers_eval/rawdata/bearb-hour/alldata.IC.nt
for f in *.gz ; do gzip -d < "$f" > /starvers_eval/rawdata/bearb-hour/alldata.IC.nt/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P /starvers_eval/rawdata/bearb-hour https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/hour/TB/alldata.TB.nq.gz
gzip -d < /starvers_eval/rawdata/bearb-hour/alldata.TB.nq.gz > /starvers_eval/rawdata/bearb-hour/alldata.TB.nq
> /starvers_eval/rawdata/bearb-hour/empty.nt # for CB policy: empty initial delete changeset

wget -t 3 -c -P /starvers_eval/rawdata/bearb-day https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/day/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers_eval/rawdata/bearb-day/alldata.IC.nt
tar -xf /starvers_eval/rawdata/bearb-day/alldata.IC.nt.tar.gz -C /starvers_eval/rawdata/bearb-day/alldata.IC.nt
cd /starvers_eval/rawdata/bearb-day/alldata.IC.nt
for f in *.gz ; do gzip -d < "$f" > /starvers_eval/rawdata/bearb-day/alldata.IC.nt/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P /starvers_eval/rawdata/bearb-day https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/day/TB/alldata.TB.nq.gz
gzip -d < /starvers_eval/rawdata/bearb-day/alldata.TB.nq.gz > /starvers_eval/rawdata/bearb-day/alldata.TB.nq
> /starvers_eval/rawdata/bearb-day/empty.nt # for CB policy: empty initial delete changeset

wget -t 3 -c -P /starvers_eval/rawdata/bearc https://aic.ai.wu.ac.at/qadlod/bear/BEAR_C/datasets/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers_eval/rawdata/bearc/alldata.IC.nt
tar -xf /starvers_eval/rawdata/bearc/alldata.IC.nt.tar.gz -C /starvers_eval/rawdata/bearc/alldata.IC.nt
cd /starvers_eval/rawdata/bearc/alldata.IC.nt
for f in *.gz ; do gzip -d < "$f" > /starvers_eval/rawdata/bearc/alldata.IC.nt/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P /starvers_eval/rawdata/bearc https://aic.ai.wu.ac.at/qadlod/bear/BEAR_C/datasets/TB/alldata.TB.nq.gz
gzip -d < /starvers_eval/rawdata/bearc/alldata.TB.nq.gz > /starvers_eval/rawdata/bearc/alldata.TB.nq
> /starvers_eval/rawdata/bearc/empty.nt # for CB policy: empty initial delete changeset