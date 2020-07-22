#!/bin/bash

# ==== WIP ====

wget "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00000.json.gz"
wget "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00001.json.gz"
wget "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00002.json.gz"
wget "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00003.json.gz"
wget "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00004.json.gz"
wget "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00005.json.gz"
wget "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00006.json.gz"
wget "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00007.json.gz"
wget "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00008.json.gz"
wget "https://d3l36jjwr70u5l.cloudfront.net/data-engineer-test/part-00009.json.gz"
mkdir data-engineer-test
mv part-*.gz data-engineer-test/
wget https://bin.equinox.io/c/4VmDzA7iaHb/ngrok-stable-linux-amd64.zip
unzip ngrok-stable-linux-amd64.zip -o .

hadoop fs -put data-engineer-test hdfs:///home/hadoop

spark-submit spark-clickstreams.py
