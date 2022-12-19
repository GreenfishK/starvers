#!/bin/bash

# Variables
baseDir=/starvers_eval
SCRIPT_DIR=/starvers_eval/scripts
policies=("ic" "tb") # only raw datasets. Don't change order!
datasets=("${datasets}") # beara bearb_hour bearb_day bearc
export JAVA_HOME=/usr/local/openjdk-11
export PATH=/usr/local/openjdk-11/bin:$PATH

# Dirs and files
mkdir -p ${baseDir}/databases/preprocessing
rm -rf ${baseDir}/databases/preprocessing/*
mkdir -p ${baseDir}/configs/preprocessing
rm -rf ${baseDir}/configs/preprocessing/*
mkdir -p $baseDir/output/logs/preprocessing/
rm -rf $baseDir/output/logs/preprocessing/*
> $baseDir/output/logs/preprocessing/exclude_invalid_triples.txt
> $baseDir/output/logs/preprocessing/skolemize_blank_nodes.txt

echo "Start corrections"
for dataset in ${datasets[@]}; do
    case $dataset in 
        beara) versions=58 file_name_struc="%01g";; 
        bearb_hour) versions=1299 file_name_struc="%06g";; 
        bearb_day) versions=89 file_name_struc="%06g";;
        bearc) versions=33 file_name_struc="%01g";;
        beart) versions=4 file_name_struc="%06g";;
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
        for c in $(seq -f $file_name_struc 1 ${versions})
        do
            #ds_dir=`eval echo $baseDir/rawdata/$dataset`
            raw_ds=`eval echo $baseDir/rawdata/$dataset/${ds_rel_path}`
            clean_ds=${raw_ds/${c}./${c}_clean.}

            # Read dataset $raw_ds line by line. 
            # If the triple is invalid write it to $clean_ds with a '#' upfront. Otherwise write the line as it is.
            # TODO: change path to $SCRIPT_DIR/2_preprocess/rdfvalidator-1.0-jar-with-dependencies.jar once you move the RDFValidator to the docker image
            repositoryID=`eval echo ${policy}_${dataset}${ds_sds_rel_pathegment}`
            invalid_lines_file=$baseDir/output/logs/preprocessing/invalid_triples_${repositoryID}.txt 
            first_line=`grep -E -m 1 '^# invalid_lines_excluded' $raw_ds`
            if [[ -z "$first_line" ]]; then
                java -jar $SCRIPT_DIR/2_preprocess/RDFValidator/target/rdfvalidator-1.0-jar-with-dependencies.jar $raw_ds $clean_ds
                mv $clean_ds $raw_ds
                excluded_lines=`grep -c '^# ' ${raw_ds}`
                sed -i "1s/^/# invalid_lines_excluded: ${excluded_lines}\n/" $raw_ds
                echo "${raw_ds}: $excluded_lines" >> $baseDir/output/logs/preprocessing/exclude_invalid_triples.txt
            else
                echo "${raw_ds}: 0 in this run. Previously excluded lines: see first comment in ${raw_ds}" >> $baseDir/output/logs/preprocessing/exclude_invalid_triples.txt
            fi

            # Skolemize blank nodes in subject position
            yn_skolemized_sub=`grep -E -m 1 '^# skolemized_blank_nodes_in_subject_position' $raw_ds`
            if [[ -z $yn_skolemized_sub ]]; then
                cnt_b_sub=`grep -c -E '(^_:[a-zA-Z0-9]+)' $raw_ds`
                sed -i -r 's/(^_:[a-zA-Z0-9]+)/<\1>/g' $raw_ds
                sed -i "2s/^/# skolemized_blank_nodes_in_subject_position: ${cnt_b_sub}\n/" $raw_ds
                echo "${raw_ds}: skolemized blank nodes in subject position: $cnt_b_sub" >> $baseDir/output/logs/preprocessing/skolemize_blank_nodes.txt
            else
                echo "${raw_ds}: skolemized blank nodes in subject position: 0 in this run. Previously skolemized nodes: See comment in ${raw_ds}" >> $baseDir/output/logs/preprocessing/skolemize_blank_nodes.txt
            fi
            
            # Skolemize blank nodes in object position
            yn_skolemized_obj=`grep -E -m 1 '^# skolemized_blank_nodes_in_object_position' $raw_ds`
            if [[ -z $yn_skolemized_obj ]]; then
                cnt_b_obj=`grep -c -E '(^[^#].*)(_:[a-zA-Z0-9]+)(\s*(<[a-zA-Z0-9_/:.]+>){0,1}\s*\.$)' $raw_ds`
                sed -i -r 's/(^[^#].*)(_:[a-zA-Z0-9]+)(\s*(<[a-zA-Z0-9_/:.]+>){0,1}\s*\.$)/\1<\2>\3/g' $raw_ds
                sed -i "2s/^/# skolemized_blank_nodes_in_object_position: ${cnt_b_obj}\n/" $raw_ds
                echo "${raw_ds}: skolemized blank nodes in object position: $cnt_b_obj" >> $baseDir/output/logs/preprocessing/skolemize_blank_nodes.txt
            else
                echo "${raw_ds}: skolemized blank nodes in object position: 0 in this run. Previously skolemized nodes: See comment in ${raw_ds}" >> $baseDir/output/logs/preprocessing/skolemize_blank_nodes.txt
            fi

        done
    done
done


