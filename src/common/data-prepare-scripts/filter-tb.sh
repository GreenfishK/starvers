while read line; do
    g=$(echo $line | sed "s/^.*$\(<http:\/\/example.org\/[^>]*>\) \./\1/")
    echo $g
done
