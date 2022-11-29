#!/bin/bash

# Variables
baseDir=/starvers_eval
SCRIPT_DIR=/starvers_eval/scripts
policies=("ic" "tb") # only raw datasets
datasets=("${datasets}") # beara bearb_hour bearb_day bearc
> $baseDir/output/logs/corrections.txt

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
            ic) datasetDirOrFile=alldata.IC.nt file_var='/${c}.nt';;
            tb) datasetDirOrFile=alldata.TB.nq versions=1 file_var='';;
            *)
                echo "Policy must be in ic or tb, which are the policies of the raw datasets."
                exit 2
            ;;
        esac
        echo "Correcting $dataset for $policy policy"
        if [ "$dataset" == "beara" ]; then
            for c in $(seq -f $file_name_struc 1 ${versions})
            do
                file=`eval echo ${file_var}`
                ic_file=$baseDir/rawdata/$dataset/${datasetDirOrFile}${file}
                echo "$ic_file"
                echo "Correct bad date, time dateTime, and duration formats"
                sed -i -r 's/("[0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{3,6}){0,1}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#dateTime>)/\1T\2/g' $ic_file
                sed -i -r 's/(")([0-9]{1}:[0-9]{2}:[0-9]{2}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#time>)/\10\2/g' $ic_file
                sed -i -r 's/(")([0-9]{1}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#(time|dateTime)>)/\1200\2/g' $ic_file
                sed -i -r 's/("[A-SU-Za-z0-9]*)("\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#duration>)/\1T0H0M0S\2/g' $ic_file
                sed -i -r 's/"([1-9])"(\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#gYear>)/"000\1"\2/g' $ic_file
                sed -i -r 's/ GMT+02:00"/+02:00"/g' $ic_file

                echo "Change datatype of wrongly formatted datatypes to string."
                sed -i -r 's/("(-[0-9]{4}-[0-9]{2}-[0-9]{2}|[A-Za-z]{0,20}\s([0-9]{2},\s){0,1}[0-9]{4}|[0-9]+|[0-9]*[A-Za-z?!\\#@/-]+[^"]*|0[^"]*|[A-Za-z]+.*)"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(date>|dateTime>)/\1string>/g' $ic_file
                sed -i -r 's/("(-[0-9]{4}|[^0-9][^"]*)"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(gYear>)/\1string>/g' $ic_file 
                sed -i -r 's/("([^0-9]+[^"]*|[0-9]+[^0-9"]*)"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(int>|double>|decimal>)/\1string>/g' $ic_file
                sed -i -r 's/(""\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)([a-zA-Z]+>)/\1string>/g' $ic_file
                sed -i -r 's/(\^\^<http:\/\/www.w3.org\/1999\/02\/22-rdf-syntax-ns#XMLLiteral>)/\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#string>/g' $ic_file
                sed -i -r 's/("[^"]*[^A-Za-z0-9]+[^"]*"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(hexBinary>)/\1string>/g' $ic_file 

                echo "Correct unescaped ampersand"  
                sed -i -r 's/(href=\\.*\?.*)(\&)(amp;){0,1}/\1\&amp;/g' $ic_file
                sed -i -r 's/(<extref href=\\"http:\/\/www.kent.ac.uk\/library\/specialcollections\/other\/search.html\?k\[0\]=PC)(\&)(amp;){0,1}/\1\&amp;/g' $ic_file
                                    
                echo "Correct wrongly assigned datatypes to the actual ones"
                sed -i -r 's/("[0-9]+"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(string>)/\1int>/g' $ic_file
                sed -i -r 's/("[0-9]+\.[0-9]+(E\+[0-9]+){0,1}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(string>)/\1double>/g' $ic_file
                sed -i -r 's/("[0-9]{4}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(dateTime>)/\1gYear>/g' $ic_file
                sed -i -r 's/("[0-9]{4}-[0-9]{2}-[0-9]{2}"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#)(dateTime>)/\1date>/g' $ic_file
                                    
                echo "Correct wrongly formatted object IRIS, subject IRIs and blank nodes."
                sed -i -r 's/(^(<[^>]*>|_:.*) <[^>]*>)( <([^h][^t][^t][^p]|[^:]*)> .$)/\1 <http:\/\/example.com\/\4> ./g' $ic_file
                sed -i -r 's/(^<)(#[^>]*> <.*> (<.*>|".*"(\^\^<.*>){0,1}) .$)/\1http:\/\/example\.com\2/g' $ic_file
                sed -i -r 's/(<)(node[A-Za-z0-9]*)(>)/_:\2/g' $ic_file

                echo "Corrected $ic_file \n" >> $baseDir/output/logs/corrections.txt
            done
        elif [ "$dataset" == "bearc" ]; then
            for c in $(seq -f $file_name_struc 1 ${versions})
            do
                file=`eval echo ${file_var}`
                ic_file=$baseDir/rawdata/$dataset/${datasetDirOrFile}${file}
                echo "$ic_file"
                echo "Correct bad IRI"
                sed -i 's/<http:\/cordis.europa.eu\/data\/cordis-fp7projects-xml.zip>/<http:\/\/cordis.europa.eu\/data\/cordis-fp7projects-xml.zip>/g' $ic_file
            
                echo "Corrected $ic_file \n" >> $baseDir/output/logs/corrections.txt
            done
        fi
    done
done


