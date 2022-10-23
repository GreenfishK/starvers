#!/bin/bash
wget -t 3 -c -P /starvers/rawdata/beara https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/datasets/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers/rawdata/beara/alldata.IC.nt
tar -xf /starvers/rawdata/beara/alldata.IC.nt.tar.gz -C /starvers/rawdata/beara/alldata.IC.nt
wget -t 3 -c -P /starvers/rawdata/beara https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/datasets/TB/alldata.TB.nq.gz
gzip -d < /starvers/rawdata/beara/alldata.TB.nq.gz > /starvers/rawdata/beara/alldata.TB.nq
> /starvers/rawdata/beara/empty.nt # for CB policy: empty initial delete changeset

wget -t 3 -c -P /starvers/rawdata/bearb-hour https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/hour/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers/rawdata/bearb-hour/alldata.IC.nt
tar -xf /starvers/rawdata/bearb-hour/alldata.IC.nt.tar.gz -C /starvers/rawdata/bearb-hour/alldata.IC.nt
cd /starvers/rawdata/bearb-hour/alldata.IC.nt
for f in *.gz ; do gzip -d < "$f" > /starvers/rawdata/bearb-hour/alldata.IC.nt/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P /starvers/rawdata/bearb-hour https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/hour/TB/alldata.TB.nq.gz
gzip -d < /starvers/rawdata/bearb-hour/alldata.TB.nq.gz > /starvers/rawdata/bearb-hour/alldata.TB.nq
> /starvers/rawdata/bearb-hour/empty.nt # for CB policy: empty initial delete changeset

wget -t 3 -c -P /starvers/rawdata/bearb-day https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/day/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers/rawdata/bearb-day/alldata.IC.nt
tar -xf /starvers/rawdata/bearb-day/alldata.IC.nt.tar.gz -C /starvers/rawdata/bearb-day/alldata.IC.nt
cd /starvers/rawdata/bearb-day/alldata.IC.nt
for f in *.gz ; do gzip -d < "$f" > /starvers/rawdata/bearb-day/alldata.IC.nt/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P /starvers/rawdata/bearb-day https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/day/TB/alldata.TB.nq.gz
gzip -d < /starvers/rawdata/bearb-day/alldata.TB.nq.gz > /starvers/rawdata/bearb-day/alldata.TB.nq
> /starvers/rawdata/bearb-day/empty.nt # for CB policy: empty initial delete changeset

wget -t 3 -c -P /starvers/rawdata/bearc https://aic.ai.wu.ac.at/qadlod/bear/BEAR_C/datasets/IC/alldata.IC.nt.tar.gz
mkdir -p /starvers/rawdata/bearc/alldata.IC.nt
tar -xf /starvers/rawdata/bearc/alldata.IC.nt.tar.gz -C /starvers/rawdata/bearc/alldata.IC.nt
cd /starvers/rawdata/bearc/alldata.IC.nt
for f in *.gz ; do gzip -d < "$f" > /starvers/rawdata/bearc/alldata.IC.nt/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P /starvers/rawdata/bearc https://aic.ai.wu.ac.at/qadlod/bear/BEAR_C/datasets/TB/alldata.TB.nq.gz
gzip -d < /starvers/rawdata/bearc/alldata.TB.nq.gz > /starvers/rawdata/bearc/alldata.TB.nq
> /starvers/rawdata/bearc/empty.nt # for CB policy: empty initial delete changeset