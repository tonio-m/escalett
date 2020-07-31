#!/bin/bash
git clone https://github.com/tonio-m/escalett
mkdir data-engineer-test
cd data-engineer-test
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
cd ../escalett
hadoop fs -put ~/data-engineer-test  hdfs:///user/hadoop/
spark-submit spark-clickstreams.py
hadoop fs -cp hdfs:///sessions/ ~/sessions
cat sessions/unique/by_file.json
cat sessions/unique/by_family.json
cat sessions/median/by_family.json
