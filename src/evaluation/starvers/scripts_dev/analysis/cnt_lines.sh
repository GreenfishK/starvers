#!/bin/sh

# Create/overwrite the output file with headers
echo "File Name,Invalid Lines,Total Lines,Invalid Lines Ratio (%)" > $HOME/beara_cnt_lines.csv

for file in /mnt/data/starvers_eval/rawdata/beara/alldata.IC.nt/*
do
    # Get the number of invalid lines
    invalid_lines=$(head -n 1 $file | grep -oP '# invalid_lines_excluded: \K\d+')
    
    # Get the total number of lines
    total_lines=$(wc -l < $file)
    
    # Calculate the invalid lines ratio
    invalid_ratio=$(awk "BEGIN { printf \"%.2f\", ($invalid_lines / $total_lines) * 100 }")
    
    # Append the data to the CSV file
    echo "$file,$invalid_lines,$total_lines,$invalid_ratio" >> $HOME/beara_cnt_lines.csv
done

