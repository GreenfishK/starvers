#!/bin/bash
wget -t 3 -c -P ~/.BEAR/rawdata/beara https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/datasets/IC/alldata.IC.nt.tar.gz
mkdir -p ~/.BEAR/rawdata/beara/alldata.IC.nt
tar -xf ~/.BEAR/rawdata/beara/alldata.IC.nt.tar.gz -C ~/.BEAR/rawdata/beara/alldata.IC.nt
wget -t 3 -c -P ~/.BEAR/rawdata/beara https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/datasets/TB/alldata.TB.nq.gz
gzip -d < ~/.BEAR/rawdata/beara/alldata.TB.nq.gz > ~/.BEAR/rawdata/beara/alldata.TB.nq
> ~/.BEAR/rawdata/beara/empty.nt # for CB policy: empty initial delete changeset

wget -t 3 -c -P ~/.BEAR/rawdata/bearb-hour https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/hour/IC/alldata.IC.nt.tar.gz
mkdir -p ~/.BEAR/rawdata/bearb-hour/alldata.IC.nt
tar -xf ~/.BEAR/rawdata/bearb-hour/alldata.IC.nt.tar.gz -C ~/.BEAR/rawdata/bearb-hour/alldata.IC.nt
cd ~/.BEAR/rawdata/bearb-hour/alldata.IC.nt
for f in *.gz ; do gzip -d < "$f" > ~/.BEAR/rawdata/bearb-hour/alldata.IC.nt/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P ~/.BEAR/rawdata/bearb-hour https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/hour/TB/alldata.TB.nq.gz
gzip -d < ~/.BEAR/rawdata/bearb-hour/alldata.TB.nq.gz > ~/.BEAR/rawdata/bearb-hour/alldata.TB.nq
> ~/.BEAR/rawdata/bearb-hour/empty.nt # for CB policy: empty initial delete changeset

wget -t 3 -c -P ~/.BEAR/rawdata/bearb-day https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/day/IC/alldata.IC.nt.tar.gz
mkdir -p ~/.BEAR/rawdata/bearb-day/alldata.IC.nt
tar -xf ~/.BEAR/rawdata/bearb-day/alldata.IC.nt.tar.gz -C ~/.BEAR/rawdata/bearb-day/alldata.IC.nt
cd ~/.BEAR/rawdata/bearb-day/alldata.IC.nt
for f in *.gz ; do gzip -d < "$f" > ~/.BEAR/rawdata/bearb-day/alldata.IC.nt/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P ~/.BEAR/rawdata/bearb-day https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/day/TB/alldata.TB.nq.gz
gzip -d < ~/.BEAR/rawdata/bearb-day/alldata.TB.nq.gz > ~/.BEAR/rawdata/bearb-day/alldata.TB.nq
> ~/.BEAR/rawdata/bearb-day/empty.nt # for CB policy: empty initial delete changeset

wget -t 3 -c -P ~/.BEAR/rawdata/bearc https://aic.ai.wu.ac.at/qadlod/bear/BEAR_C/datasets/IC/alldata.IC.nt.tar.gz
mkdir -p ~/.BEAR/rawdata/bearc/alldata.IC.nt
tar -xf ~/.BEAR/rawdata/bearc/alldata.IC.nt.tar.gz -C ~/.BEAR/rawdata/bearc/alldata.IC.nt
cd ~/.BEAR/rawdata/bearc/alldata.IC.nt
for f in *.gz ; do gzip -d < "$f" > ~/.BEAR/rawdata/bearc/alldata.IC.nt/"${f%.*}" ; done
rm *.gz
wget -t 3 -c -P ~/.BEAR/rawdata/bearc https://aic.ai.wu.ac.at/qadlod/bear/BEAR_C/datasets/TB/alldata.TB.nq.gz
gzip -d < ~/.BEAR/rawdata/bearc/alldata.TB.nq.gz > ~/.BEAR/rawdata/bearc/alldata.TB.nq
> ~/.BEAR/rawdata/bearc/empty.nt # for CB policy: empty initial delete changeset