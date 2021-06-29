#!/bin/bash
dir=tdb-bearb-hour/cb
for file in $(ls -v $dir/* | sed -n "/:/p" | sed "s/://"); do
    #v=$(echo $file | sed 's/^.*\/\([0-9]*\)\..*$/\1/')
    v=$(echo $file | sed 's/^.*\/\([0-9]*\)$/\1/')
    vp=$(echo "$v-1" | bc)
    target=$(echo $file | sed "s/$v/$vp/")
    mv $file $target
done
