#!/bin/bash

# Variables
baseDir=/starvers_eval
SCRIPT_DIR=/starvers_eval/scripts
policies=("ic" "tb") # only raw datasets. Don't change order!
datasets=("${datasets}") # beara bearb_hour bearb_day bearc
export JAVA_HOME=/usr/local/openjdk-11
export PATH=/usr/local/openjdk-11/bin:$PATH
export FUSEKI_HOME=/jena-fuseki
export _JAVA_OPTIONS="-Xmx90g"

# Dirs and files
#> $baseDir/output/logs/corrections.txt
mkdir -p ${baseDir}/databases/preprocessing
rm -rf ${baseDir}/databases/preprocessing/*
mkdir -p ${baseDir}/configs/preprocessing
rm -rf ${baseDir}/configs/preprocessing/*
mkdir -p $baseDir/output/logs/preprocessing/
rm -rf $baseDir/output/logs/preprocessing/*

echo "Start corrections"
for dataset in ${datasets[@]}; do
    case $dataset in 
        beara) versions=58 file_name_struc="%01g";; 
        bearb_hour) versions=1299 file_name_struc="%06g";; 
        bearb_day) versions=89 file_name_struc="%06g";;
        bearc) versions=33 file_name_struc="%01g";;
        *)
            echo "graphdb: Dataset must be in beara bearb_hour bearb_day bearc"
            exit 2
        ;;
    esac

    for policy in ${policies[@]}; do
        case $policy in 
            ic) ds_rel_path='alldata.IC.nt/${c}.nt' ds_segment='_${c}';;
            tb) ds_rel_path='alldata.TB.nq' versions=1 ds_segment='';;
            *)
                echo "Policy must be in ic or tb, which are the policies of the raw datasets."
                exit 2
            ;;
        esac
        echo "Correcting $dataset for $policy policy"
        mkdir -p ${baseDir}/configs/preprocessing/jenatdb2_${policy}_${dataset}
        for c in $(seq -f $file_name_struc 1 ${versions})
        do
            ds_abs_path=`eval echo $baseDir/rawdata/$dataset/${ds_rel_path}`

            # copy config template
            repositoryID=`eval echo ${policy}_${dataset}${ds_segment}`
            echo $repositoryID
            cp ${SCRIPT_DIR}/1_get_and_prepare_data/configs/jenatdb2-config_template.ttl ${baseDir}/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
            sed -i "s/{{repositoryID}}/$repositoryID/g" ${baseDir}/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
            sed -i "s/{{policy}}/$policy/g" ${baseDir}/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
            sed -i "s/{{dataset}}/$dataset/g" ${baseDir}/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl

            # Write the line number of every invalid triple into a file
            invalid_lines_file=$baseDir/output/logs/preprocessing/invalid_triples_${repositoryID}.txt 
            # TODO: change path to $SCRIPT_DIR/1_get_and_prepare_data/rdfvalidator-1.0-jar-with-dependencies.jar once you move the RDFValidator to the docker image
            java -jar $SCRIPT_DIR/1_get_and_prepare_data/RDFValidator/target/rdfvalidator-1.0-jar-with-dependencies.jar $ds_abs_path $invalid_lines_file

            # Exclude invalid lines by out-commenting them in the original file
            invalid_lines=`cat $invalid_lines_file`
            substitutions=""
            for invalid_line in $invalid_lines
            do
                substitutions="${substitutions}${invalid_line}s/(.*)/# \1/g;"
            done
            sed -i -r "$substitutions" $ds_abs_path
            # TODO: Include lines that are already excluded via hashtags at the beginning of the log file
            #commented_out_lines=`grep -n -E "^2" invalid_triples_ic_beara_1.txt | cut -f1 -d:`
            #sed -i "1i $commented_out_lines" $invalid_lines_file

            if [ -z "$invalid_line" ]; then
                echo "$ds_abs_path has no errors according to the jena RDFParser."
            else
                cnt_excluded=`sed -n "$=" $invalid_lines_file`
                echo "$cnt_excluded excluded via commenting (hashtag) from $ds_abs_path"
            fi
        done
    done
done


