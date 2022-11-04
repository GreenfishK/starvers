#!/bin/bash

# Variables
baseDir=/starvers_eval
SCRIPT_DIR=/starvers_eval/scripts
policies=("ic tb") # only raw datasets
datasets=("${datasets}") # beara bearb_hour bearb_day bearc

for policy in ${policies[@]}; do
    case $policy in 
        ic) datasetDirOrFile=alldata.IC.nt;;
        tb) datasetDirOrFile=alldata.TB.nq;;
        *)
            echo "Policy must be in ic or tb, which are the policies of the raw datasets."
            exit 2
        ;;
    esac

    for dataset in ${datasets[@]}; do
        echo $dataset
        case $dataset in 
            beara) versions=1 file_name_struc="%01g";; #versions=58
            bearb_hour) versions=1299 file_name_struc="%06g";; 
            bearb_day) versions=89 file_name_struc="%06g";;
            bearc) versions=32 file_name_struc="%01g";;
            *)
                echo "graphdb: Dataset must be in beara bearb_hour bearb_day bearc"
                exit 2
            ;;
        esac
        if [ "$policy" == "ic"]; then
            if [ "$dataset" == "beara"]; then
                # Correct BEARA IC
                for c in $(seq -f $file_name_struc 1 ${versions})
                do
                    ic_file = $baseDir/rawdata/$dataset/$datasetDirOrFile/${c}.nt
                    # Bad timestamp format
                    sed -i 's/ GMT+02:00"/+02:00/g' ic_file
                    # Ampersand not escaped
                    sed -i 's/<extref href=\\"http:\/\/opac.kent.ac.uk\/cgi-bin\/Pwebrecon.cgi?DB=local\&PAGE=First/<extref href=\\"http:\/\/opac.kent.ac.uk\/cgi-bin\/Pwebrecon.cgi?DB=local\&amp;PAGE=First/g'
                    # Bad blank nodes format
                    sed -i 's/\(<\)\(node.*\)\(>\)/_:\2/g'
                done
            elif [ "$dataset" == "bearc"]; then
                # Correct BEARC IC
                for c in $(seq -f $file_name_struc 1 ${versions})
                do
                # Bad IRI
                sed -i 's/<http:\/cordis.europa.eu\/data\/cordis-fp7projects-xml.zip>/<http:\/\/cordis.europa.eu\/data\/cordis-fp7projects-xml.zip>/g' ic_file
                done
            fi
        fi
done

