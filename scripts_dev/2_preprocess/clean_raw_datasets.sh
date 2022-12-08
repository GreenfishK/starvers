#!/bin/bash

# Variables
baseDir=/starvers_eval
SCRIPT_DIR=/starvers_eval/scripts
policies=("ic" "tb") # only raw datasets. Don't change order!
datasets=("${datasets}") # beara bearb_hour bearb_day bearc

# Dirs and files
mkdir -p ${baseDir}/databases/preprocessing
rm -rf ${baseDir}/databases/preprocessing/*
mkdir -p ${baseDir}/configs/preprocessing
rm -rf ${baseDir}/configs/preprocessing/*
mkdir -p $baseDir/output/logs/preprocessing/
rm -rf $baseDir/output/logs/preprocessing/*
> $baseDir/output/logs/preprocessing/clean_raw_datasets.txt
> $baseDir/output/logs/preprocessing/clean_raw_datasets_blank_nodes.txt

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
        for c in $(seq -f $file_name_struc 1 ${versions})
        do
            ds_abs_path=`eval echo $baseDir/rawdata/$dataset/${ds_rel_path}`

            # copy config template
            repositoryID=`eval echo ${policy}_${dataset}${ds_segment}`
            echo $repositoryID

            # Match invalid triples and save the line numbers to invalid_lines_file
            invalid_lines_file=$baseDir/output/logs/preprocessing/invalid_triples_${repositoryID}.txt 
            # TODO: change path to $SCRIPT_DIR/2_preprocess/rdfvalidator-1.0-jar-with-dependencies.jar once you move the RDFValidator to the docker image
            java -jar $SCRIPT_DIR/2_preprocess/RDFValidator/target/rdfvalidator-1.0-jar-with-dependencies.jar $ds_abs_path $invalid_lines_file

            # Exclude invalid lines by out-commenting them in the original file
            invalid_lines=`cat $invalid_lines_file`
            substitutions=""
            for invalid_line in $invalid_lines
            do
                substitutions="${substitutions}${invalid_line}s/(.*)/# \1/g;"
            done
            # Exclude invalid triples
            sed -i -r "$substitutions" $ds_abs_path
            # Skolemize blank nodes in subject position
            cnt_b_sub=`grep -c -E '(^_:[a-zA-Z0-9]+)' $ds_abs_path`
            sed -i -r 's/(^_:[a-zA-Z0-9]+)/<\1>/g' $ds_abs_path
            # Skolemize blank nodes in object position
            cnt_b_obj=`grep -c -E '(^[^#].*)(_:[a-zA-Z0-9]+)(\s*(<[a-zA-Z0-9_/:.]+>){0,1}\s*\.$)' $ds_abs_path`
            sed -i -r 's/(^[^#].*)(_:[a-zA-Z0-9]+)(\s*(<[a-zA-Z0-9_/:.]+>){0,1}\s*\.$)/\1<\2>\3/g' $ds_abs_path
            echo "${ds_abs_path}: skolemized blank nodesin subject position: $cnt_b_sub" >> $baseDir/output/logs/preprocessing/clean_raw_datasets_blank_nodes.txt
            echo "${ds_abs_path}: skolemized blank nodesin object position: $cnt_b_obj" >> $baseDir/output/logs/preprocessing/clean_raw_datasets_blank_nodes.txt

            # Print how many lines were excluded in this run
            if [ -z "$invalid_lines" ]; then
                echo "$ds_abs_path has no errors according to the jena RDFParser."
            else
                cnt_excluded=`sed -n "$=" $invalid_lines_file`
                echo "$cnt_excluded excluded via commenting (hashtag) from $ds_abs_path"
            fi

            # Log how many lines are excluded from the dataset
            excluded_lines=`grep -c '^# ' $ds_abs_path`
            echo "${ds_abs_path}: $excluded_lines" >> $baseDir/output/logs/preprocessing/clean_raw_datasets.txt
        done
    done
done


