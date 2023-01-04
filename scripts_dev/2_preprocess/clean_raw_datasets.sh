#!/bin/bash

# Set variables
SCRIPT_DIR=/starvers_eval/scripts
policies=("ic" "tb") # only raw datasets. Don't change order!
datasets=("${datasets}") # beara bearb_hour bearb_day bearc
log_file=/starvers_eval/output/logs/preprocessing/clean_datasets.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"
export JAVA_HOME=/usr/local/openjdk-11
export PATH=/usr/local/openjdk-11/bin:$PATH

# Clean directories and files
rm -rf /starvers_eval/databases/preprocessing
rm -rf /starvers_eval/configs/preprocessing
rm -rf /starvers_eval/output/logs/preprocessing

# Create directories and files
mkdir -p /starvers_eval/configs/preprocessing
mkdir -p /starvers_eval/databases/preprocessing
mkdir -p /starvers_eval/output/logs/preprocessing
> $log_file

echo "$(log_timestamp) ${log_level}:Start corrections" >> $log_file
for dataset in ${datasets[@]}; do
    case $dataset in 
        beara) versions=58 file_name_struc="%01g";; 
        bearb_hour) versions=1299 file_name_struc="%06g";; 
        bearb_day) versions=89 file_name_struc="%06g";;
        bearc) versions=33 file_name_struc="%01g";;
        beart) versions=4 file_name_struc="%06g";;
        *)
            echo "$(log_timestamp) ${log_level}:graphdb: Dataset must be in beara bearb_hour bearb_day bearc" >> $log_file
            exit 2
        ;;
    esac

    for policy in ${policies[@]}; do
        case $policy in 
            ic) ds_rel_path='alldata.IC.nt/${c}.nt' base_name_tmpl='${c}.nt';;
            tb) ds_rel_path='alldata.TB.nq' versions=1 base_name_tmpl='alldata.TB';;
            *)
                echo "$(log_timestamp) ${log_level}:Policy must be in ic or tb, which are the policies of the raw datasets." >> $log_file
                exit 2
            ;;
        esac
        echo "$(log_timestamp) ${log_level}:Correcting $dataset for $policy policy" >> $log_file
        for c in $(seq -f $file_name_struc 1 ${versions})
        do
            base_name=`eval echo $base_name_tmpl`
            raw_ds=`eval echo /starvers_eval/rawdata/$dataset/${ds_rel_path}`
            clean_ds=${raw_ds/${base_name}/${base_name}_clean}

            # Skolemize blank nodes in subject position
            yn_skolemized_sub=`head -3 $raw_ds | grep -E -m 1 '^# skolemized_blank_nodes_in_subject_position'`
            if [[ -z $yn_skolemized_sub ]]; then
                cnt_b_sub=`grep -c -E '(^_:[a-zA-Z0-9]+)' $raw_ds`
                sed -i -r 's/(^_:[a-zA-Z0-9]+)/<\1>/g' $raw_ds
                sed -i "1s/^/# skolemized_blank_nodes_in_subject_position: ${cnt_b_sub}\n/" $raw_ds
                echo "$(log_timestamp) ${log_level}:${raw_ds}: skolemized blank nodes in subject position: $cnt_b_sub" >> $log_file
            else
                echo "$(log_timestamp) ${log_level}:${raw_ds}: skolemized blank nodes in subject position: 0 in this run. Previously skolemized nodes: See comment in ${raw_ds}" >> $log_file
            fi
            
            # Skolemize blank nodes in object position
            yn_skolemized_obj=`head -3 $raw_ds | grep -E -m 1 '^# skolemized_blank_nodes_in_object_position'`
            if [[ -z $yn_skolemized_obj ]]; then
                cnt_b_obj=`grep -c -E '(^[^#].*)(_:[a-zA-Z0-9]+)(\s*(<[a-zA-Z0-9_/:.]+>){0,1}\s*\.$)' $raw_ds`
                sed -i -r 's/(^[^#].*)(_:[a-zA-Z0-9]+)(\s*(<[a-zA-Z0-9_/:.]+>){0,1}\s*\.$)/\1<\2>\3/g' $raw_ds
                sed -i "1s/^/# skolemized_blank_nodes_in_object_position: ${cnt_b_obj}\n/" $raw_ds
                echo "$(log_timestamp) ${log_level}:${raw_ds}: skolemized blank nodes in object position: $cnt_b_obj" >> $log_file
            else
                echo "$(log_timestamp) ${log_level}:${raw_ds}: skolemized blank nodes in object position: 0 in this run. Previously skolemized nodes: See comment in ${raw_ds}" >> $log_file
            fi

            # Read dataset $raw_ds line by line. 
            # If the triple is invalid write it to $clean_ds with a '#' upfront. Otherwise write the line as it is.
            # TODO: change path to $SCRIPT_DIR/2_preprocess/rdfvalidator-1.0-jar-with-dependencies.jar once you move the RDFValidator to the docker image
            echo "$(log_timestamp) ${log_level}:Validating $raw_ds" >> $log_file
            first_line=`head -3 $raw_ds | grep -E -m 1 '^# invalid_lines_excluded'`
            if [[ -z "$first_line" ]]; then
                java -jar $SCRIPT_DIR/2_preprocess/RDFValidator/target/rdfvalidator-1.0-jar-with-dependencies.jar $raw_ds $clean_ds
                mv $clean_ds $raw_ds
                excluded_lines=`grep -c '^# ' ${raw_ds}`
                excluded_lines=$(($excluded_lines - 2))
                sed -i "1s/^/# invalid_lines_excluded: ${excluded_lines}\n/" $raw_ds
                echo "$(log_timestamp) ${log_level}:${raw_ds}: $excluded_lines" >> $log_file
            else
                echo "$(log_timestamp) ${log_level}:${raw_ds}: 0 in this run. Previously excluded lines: see first comment in ${raw_ds}" >> $log_file
            fi

        done
    done
done


