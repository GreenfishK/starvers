#!/bin/bash

# Variables
baseDir=/starvers_eval
SCRIPT_DIR=/starvers_eval/scripts
policies=("ic" "tb") # only raw datasets
datasets=("${datasets}") # beara bearb_hour bearb_day bearc
export JAVA_HOME=/usr/local/openjdk-11
export PATH=/usr/local/openjdk-11/bin:$PATH
export FUSEKI_HOME=/jena-fuseki

# Dirs and files
> $baseDir/output/logs/corrections.txt
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
        if [ "$dataset" == "beara" ]; then
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
                
                # Load dataset and extract line number of invalid triple
                > $baseDir/output/logs/preprocessing/invalid_triples_${repositoryID}.txt
                invalid_line=`/jena-fuseki/tdbloader2 --loc ${baseDir}/databases/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID} $ds_abs_path`
                echo $invalid_line
                # sed -i -r "${n}s/(.*)/# \1/g" # 206, 207, 208
                #echo "Correct bad date, time dateTime, and duration formats"
                #sed -i -r 's/("[0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{3,6}){0,1}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#dateTime>)/\1T\2/g' $ds_file
                #sed -i -r 's/(")([0-9]{1}:[0-9]{2}:[0-9]{2}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#time>)/\10\2/g' $ds_file
                #sed -i -r 's/(")([0-9]{1}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#(time|dateTime)>)/\1200\2/g' $ds_file
                #sed -i -r 's/("[A-SU-Za-z0-9]*)("\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#duration>)/\1T0H0M0S\2/g' $ds_file
                #sed -i -r 's/"([1-9])"(\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#gYear>)/"000\1"\2/g' $ds_file
                #sed -i -r 's/ GMT+02:00"/+02:00"/g' $ds_file

                #echo "Change datatype of wrongly formatted datatypes to string."
                #sed -i -r 's/("(-[0-9]{4}-[0-9]{2}-[0-9]{2}|[A-Za-z]{0,20}\s([0-9]{2},\s){0,1}[0-9]{4}|[0-9]+|[0-9]*[A-Za-z?!\\#@/-]+[^"]*|0[^"]*|[A-Za-z]+.*)"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(date>|dateTime>)/\1string>/g' $ds_file
                #sed -i -r 's/("(-[0-9]{4}|[^0-9][^"]*)"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(gYear>)/\1string>/g' $ds_file 
                #sed -i -r 's/("([^0-9]+[^"]*|[0-9]+[^0-9"]*)"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(int>|double>|decimal>)/\1string>/g' $ds_file
                #sed -i -r 's/(""\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)([a-zA-Z]+>)/\1string>/g' $ds_file
                #sed -i -r 's/(\^\^<http:\/\/www.w3.org\/1999\/02\/22-rdf-syntax-ns#XMLLiteral>)/\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#string>/g' $ds_file
                #sed -i -r 's/("[^"]*[^A-Za-z0-9]+[^"]*"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(hexBinary>)/\1string>/g' $ds_file 

                #echo "Correct unescaped ampersand"  
                #sed -i -r 's/(href=\\.*\?.*)(\&)(amp;){0,1}/\1\&amp;/g' $ds_file
                #sed -i -r 's/(<extref href=\\"http:\/\/www.kent.ac.uk\/library\/specialcollections\/other\/search.html\?k\[0\]=PC)(\&)(amp;){0,1}/\1\&amp;/g' $ds_file
                                    
                #echo "Correct wrongly assigned datatypes to the actual ones"
                #sed -i -r 's/("[0-9]+"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(string>)/\1int>/g' $ds_file
                #sed -i -r 's/("[0-9]+\.[0-9]+(E\+[0-9]+){0,1}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(string>)/\1double>/g' $ds_file
                #sed -i -r 's/("[0-9]{4}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(dateTime>)/\1gYear>/g' $ds_file
                #sed -i -r 's/("[0-9]{4}-[0-9]{2}-[0-9]{2}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(dateTime>)/\1date>/g' $ds_file
                                    
                #echo "Correct wrongly formatted object IRIS, subject IRIs and blank nodes."
                #sed -i -r 's/(^(<[^>]*>|_:.*) <[^>]*>)( <([^h][^t][^t][^p]|[^:]*|http:[^/][^>]*)> .$)/\1 <http:\/\/example.com\/\4> ./g' $ds_file
                #sed -i -r 's/(^(<[^>]*>|_:.*) <[^>]*>)( <([^h][^t][^t][^p]|[^:]*)> .$)/\1 <http:\/\/example.com\/\4> ./g' $ds_file
                #sed -i -r 's/(^<)(#[^>]*> <.*> (<.*>|".*"(\^\^<.*>){0,1}) .$)/\1http:\/\/example\.com\2/g' $ds_file
                #sed -i -r 's/(<)(node[A-Za-z0-9]*)(>)/_:\2/g' $ds_file

                #echo "Corrected $ds_file \n" >> $baseDir/output/logs/corrections.txt
            done
        elif [ "$dataset" == "bearc" ]; then
            for c in $(seq -f $file_name_struc 1 ${versions})
            do
                file=`eval echo ${file_var}`
                ds_file=$baseDir/rawdata/$dataset/${ds_rel_path}${file}
                echo "$ds_file"
                echo "Correct bad IRI"
                sed -i 's/<http:\/cordis.europa.eu\/data\/cordis-fp7projects-xml.zip>/<http:\/\/cordis.europa.eu\/data\/cordis-fp7projects-xml.zip>/g' $ds_file
            
                echo "Corrected $ds_file \n" >> $baseDir/output/logs/corrections.txt
            done
        fi
    done
done


