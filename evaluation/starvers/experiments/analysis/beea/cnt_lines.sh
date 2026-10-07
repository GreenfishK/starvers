#!/bin/sh

# Output file lives next to this script so the cnt_lines.sh -> beara_cnt_lines.csv ->
# statistics.py pipeline runs from a single, self-contained directory.
OUT_FILE="$(dirname "$0")/beara_cnt_lines.csv"

# Create/overwrite the output file with headers
echo "File Name,Invalid Lines,Total Lines,Invalid Lines Ratio (%)" > "$OUT_FILE"

for file in /mnt/data_local/starvers_eval/rawdata/beara/alldata.IC.nt/*
do
    # Get the number of invalid lines
    invalid_lines=$(head -n 1 $file | grep -oP '# invalid_lines_excluded: \K\d+')
    
    # Get the total number of lines
    total_lines=$(wc -l < $file)
    
    # Calculate the invalid lines ratio
    invalid_ratio=$(awk "BEGIN { printf \"%.2f\", ($invalid_lines / $total_lines) * 100 }")
    
    # Append the data to the CSV file
    echo "$file,$invalid_lines,$total_lines,$invalid_ratio" >> "$OUT_FILE"
done

