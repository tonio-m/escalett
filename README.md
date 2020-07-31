# EscaleTT

## Para avaliação
Link para o colab abaixo:

[spark-clickstreams.ipynb](https://colab.research.google.com/drive/1dzZHnj6gsmDBngyFpXeIsPrGgdLZA0iX?usp=sharing)


## Para rodar no cluster
1 - Crie um Cluster de spark no EMR:

```sh
aws emr create-cluster \
  --applications \
  Name=Spark \
  Name=Zeppelin \ --ec2-attributes \
  '{"KeyName":"xxxxx-ec2-key","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-xxxxxxxx","EmrManagedSlaveSecurityGroup":"sg-xxxxxxxxxxxxxxxxx","EmrManagedMasterSecurityGroup":"sg-xxxxxxxxxxxxxxxxx"}' \
  --service-role EMR_DefaultRole \
  --enable-debugging \
  --release-label emr-5.30.1 \
  --log-uri 's3n://aws-logs-xxxxxx-us-east-1/elasticmapreduce/' \
  --name 'My cluster' \
  --instance-groups \
  '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m4.large","Name":"Core Instance Group"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m4.large","Name":"Master Instance Group"}]' \
  --configurations '[{"Classification":"spark","Properties":{}}]' \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --region us-east-1
```
[ou crie no console](https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#quick-create:)

2 - Rode o job de spark
```sh
ssh -i ~/xxxxx-ec2-key.pem hadoop@xxx.xxx.x.x
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
```
