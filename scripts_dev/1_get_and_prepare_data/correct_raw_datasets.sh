#!/bin/bash

# Variables
baseDir=/starvers_eval
SCRIPT_DIR=/starvers_eval/scripts
policies=("ic" "tb") # only raw datasets
datasets=("${datasets}") # beara bearb_hour bearb_day bearc

echo "Start corrections"
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
        case $dataset in 
            beara) versions=58 file_name_struc="%01g";; 
            bearb_hour) versions=1299 file_name_struc="%06g";; 
            bearb_day) versions=89 file_name_struc="%06g";;
            bearc) versions=32 file_name_struc="%01g";;
            *)
                echo "graphdb: Dataset must be in beara bearb_hour bearb_day bearc"
                exit 2
            ;;
        esac
        if [ "$policy" == "ic" ]; then
            if [ "$dataset" == "beara" ]; then
                echo "Correcting $dataset for $policy policy"
                for c in $(seq -f $file_name_struc 1 ${versions})
                do
                    ic_file=$baseDir/rawdata/$dataset/$datasetDirOrFile/${c}.nt
                    echo "$ic_file"
                    echo "Correct bad timestamp formats"
                    sed -i 's/ GMT+02:00"/+02:00"/g' $ic_file
                    sed -i -r 's/("-)([0-9]{4}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#gYear>)/"\2/g' $ic_file
                    sed -i -r 's/"([1-9])"(\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#gYear>)/"000\1"\2/g' $ic_file
                    sed -i -r 's/("[0-9]{4}-[0-9]{2}-[0-9]{2}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(dateTime>)/\1date>/g' $ic_file
                    sed -i -r 's/("[0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{3,6}){0,1}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#dateTime>)/\1T\2/g' $ic_file
                    sed -i -r 's/("[0-9]{4}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(dateTime>)/\1gYear>/g' $ic_file
                    sed -i -r 's/(")([0-9]{1}:[0-9]{2}:[0-9]{2}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#time>)/\10\2/g' $ic_file
                    sed -i -r 's/(")([0-9]{1}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#(time|dateTime)>)/\1200\2/g' $ic_file
                    sed -i -r 's/("[A-Za-z]+.*"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(dateTime>)/\1string>/g' $ic_file
                    sed -i -r 's/("[A-SU-Za-z0-9]*)("\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#duration>)/\1T0H0M0S\2/g' $ic_file
                    echo "Change XMLLiteral to string because many literals are not well-formed for XMLLiteral."
                    sed -i -r 's/(\^\^<http:\/\/www.w3.org\/1999\/02\/22-rdf-syntax-ns#XMLLiteral>)/\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#string>/g' $ic_file
                    echo "Correct unescaped ampersand"  
                    sed -i -r 's/(href=\\.*\?.*)(\&)(amp;){0,1}/\1\&amp;/g' $ic_file
                    sed -i -r 's/(<extref href=\\"http:\/\/www.kent.ac.uk\/library\/specialcollections\/other\/search.html\?k\[0\]=PC)(\&)(amp;){0,1}/\1\&amp;/g' $ic_file
                    echo "Correct bad blank nodes format" 
                    sed -i -r 's/(<)(node[A-Za-z0-9]*)(>)/_:\2/g' $ic_file
                    echo "Correct strings that are labeled as ints, floats or dateTimes"
                    sed -i -r 's/(".*"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(int>)/\1string>/g' $ic_file
                    sed -i -r 's/(".*"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(double>)/\1string>/g' $ic_file
                    sed -i -r 's/("[0-9]+"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(string>)/\1int>/g' $ic_file
                    sed -i -r 's/("[0-9]+\.[0-9]+(E\+[0-9]+){0,1}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(string>)/\1double>/g' $ic_file
                    sed -i -r 's/(""\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(integer>|dateTime>|double>|int>|duration>)/\1string>/g' $ic_file
                    echo "Correct hexBinary"
                    sed -i -r 's/("\s*(\\n){0,1}\s*)([A-Za-z0-9]*)(\s*\\n\s*")(\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#hexBinary>)/"\3"\5/g' $ic_file
                done
            elif [ "$dataset" == "bearb_hour" ]; then
                ic_file=$baseDir/rawdata/$dataset/$datasetDirOrFile/93.nt
                sed -i -r 's/(<http:\/\/dbpedia\.org\/resource\/Rodeo_\(Travis_Scott_album\)> <http:\/\/dbpedia\.org\/property\/cover> "\{\\\\rtf1\\\\ansi\\\\ansicpg1252\{\\\\fonttbl\}\\n\{\\\\colortbl;\\\\red255\\\\green255\\\\blue255;")(@en)/\1\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#string>/g' $ic_file
            elif [ "$dataset" == "bearc" ]; then
                echo "Correcting $dataset for $policy policy"
                for c in $(seq -f $file_name_struc 1 ${versions})
                do
                    ic_file=$baseDir/rawdata/$dataset/$datasetDirOrFile/${c}.nt
                    echo "$ic_file"
                    echo "Correct bad IRI"
                    sed -i 's/<http:\/cordis.europa.eu\/data\/cordis-fp7projects-xml.zip>/<http:\/\/cordis.europa.eu\/data\/cordis-fp7projects-xml.zip>/g' $ic_file
                done
            fi
        fi
    done
done

