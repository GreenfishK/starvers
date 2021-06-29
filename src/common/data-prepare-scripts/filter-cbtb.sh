#!/bin/bash
# Limit to version 0 to 9
i="rawdata/alldata.CBTB.nq"
o="rawdata/alldata.CBTB.small.nq"

sed '/http:\/\/example.org\/version0[01][0-9]/!d' $i > $o
