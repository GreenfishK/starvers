#!/bin/bash

raw_queries_dir=~/.BEAR/queries/raw_queries/
output_queries_dir=~/.BEAR/queries/final_queries/
datasets="beara bearb bearc"
policies="ic cb tb tbsf tbsh"
input_representations="ts bgp"

mkdir -p ${output_queries_dir}/IC/queries_beara/high
mkdir -p ${output_queries_dir}/IC/queries_beara/low
mkdir -p ${output_queries_dir}/IC/queries_bearb/join
mkdir -p ${output_queries_dir}/IC/queries_bearb/lookup
mkdir -p ${output_queries_dir}/IC/queries_bearc
mkdir -p ${output_queries_dir}/CB/queries_beara/high
mkdir -p ${output_queries_dir}/CB/queries_beara/low
mkdir -p ${output_queries_dir}/CB/queries_bearb/join
mkdir -p ${output_queries_dir}/CB/queries_bearb/lookup
mkdir -p ${output_queries_dir}/CB/queries_bearc
mkdir -p ${output_queries_dir}/TB/queries_beara/high
mkdir -p ${output_queries_dir}/TB/queries_beara/low
mkdir -p ${output_queries_dir}/TB/queries_bearb/join
mkdir -p ${output_queries_dir}/TB/queries_bearb/lookup
mkdir -p ${output_queries_dir}/TB/queries_bearc
mkdir -p ${output_queries_dir}/TBSF/queries_beara/high
mkdir -p ${output_queries_dir}/TBSF/queries_beara/low
mkdir -p ${output_queries_dir}/TBSF/queries_bearb/join
mkdir -p ${output_queries_dir}/TBSF/queries_bearb/lookup
mkdir -p ${output_queries_dir}/TBSF/queries_bearc
mkdir -p ${output_queries_dir}/TBSH/queries_beara/high
mkdir -p ${output_queries_dir}/TBSH/queries_beara/low
mkdir -p ${output_queries_dir}/TBSH/queries_bearb/join
mkdir -p ${output_queries_dir}/TBSH/queries_bearb/lookup
mkdir -p ${output_queries_dir}/TBSH/queries_bearc


docker run \
       -it \
       --rm \
       -v ${raw_queries_dir}:/var/data/raw_queries/ \
       -v ${output_queries_dir}:/var/data/output_queries/ \
       bear-rdfstarstores \
       java -cp target/rdfstoreQuery-0.8.jar org/ai/wu/ac/at/rdfstarArchive/tools/GenerateQueries \
            -r /var/data/raw_queries/IC/queries_beara/high \
            -w /var/data/output_queries/IC/queries_beara/high \
            -i ts \
            -o ic \
            -v 1299 \

#-r ~/Uni/PhD/Semantic_Technologies/RDFVersioningEvaluation/rdfostrich_BEAR/data/queries/raw_queries/queries_beara/high/lookup_queries_o_high.txt 
