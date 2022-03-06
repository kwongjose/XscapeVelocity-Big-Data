#!/bin/sh

# as a filter (stdin to stdout),
# sort transformed EPA air quality data by dateTimeGMT then siteCode

read header
echo $header
key=`echo $header | cut -d , -f 1,2`
if [ "siteCode,dateTimeGMT" != "$key" ]
then
    echo "Invalid header" 1>&2
    exit 1
fi

sort -t, -k 2,2 -k 1,1

