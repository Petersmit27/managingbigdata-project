#!/bin/bash

for year in $(seq 2018 2023);
do
    echo "---------- Starting $year ----------"
    for month in $(seq -f "%02g" 1 12);
    do
        echo "Downloading clickstream-enwiki-$year-$month.tsv.gz..."
        wget https://dumps.wikimedia.org/other/clickstream/$year-$month/clickstream-enwiki-$year-$month.tsv.gz
        echo "Moving clickstream-enwiki-$year-$month.tsv.gz to DFS..."
        hdfs dfs -copyFromLocal ./clickstream-enwiki-$year-$month.tsv.gz ./clickstream/
        rm clickstream-enwiki-$year-$month.tsv.gz
    done
    echo "---------- Done with $year ----------"
done

echo "Done!"