#!/bin/bash

# Variables
baseDir=/starvers_eval
SCRIPT_DIR=/starvers_eval/scripts
policies=("tbsf" "tbsh" "cb" "icng" "cbng") # only computed/constructed datasets
datasets=("${datasets}") # beara bearb_hour bearb_day bearc

echo "Start corrections"
for policy in ${policies[@]}; do
    case $policy in 
        tbsf) datasetDirOrFile=alldata.TB_star_flat.ttl;;
        tbsh) datasetDirOrFile=alldata.TB_star_hierarchical.ttl;;
        *)
            echo "Policy must be in ic or tb, which are the policies of the raw datasets."
            exit 2
        ;;
    esac
    for dataset in ${datasets[@]}; do
        case $dataset in 
            beara) versions=58 file_name_struc="%01g";; #versions=58
            bearb_hour) versions=1299 file_name_struc="%06g";; 
            bearb_day) versions=89 file_name_struc="%06g";;
            bearc) versions=32 file_name_struc="%01g";;
            *)
                echo "graphdb: Dataset must be in beara bearb_hour bearb_day bearc"
                exit 2
            ;;
        esac
        if [ "$policy" == "tbsh" ]; then
            if [ "$dataset" == "bearb_hour" ]; then
                echo "Correcting $dataset for $policy policy"
                for c in $(seq -f $file_name_struc 1 ${versions})
                do
                    ic_file=$baseDir/rawdata/$dataset/$datasetDirOrFile
                    echo "$ic_file"
                    # TODO
                done
            fi
        elif [ "$policy" == "tbsf" ]; then
            if [ "$dataset" == "bearb_hour" ]; then
                echo "Correcting $dataset for $policy policy"
                for c in $(seq -f $file_name_struc 1 ${versions})
                do
                    ic_file=$baseDir/rawdata/$dataset/$datasetDirOrFile
                    echo "$ic_file"
                    # TODO

                done
            fi
        fi
    done
done

