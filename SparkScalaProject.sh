#!/bin/bash
if [ ! -d /user/cloudera/ ]; then
	hadoop fs -mkdir /user/cloudera
	hadoop fs -chown cloudera /user/cloudera
fi


if [ ! -d /user/cloudera/input/ ]; then
	hadoop fs -mkdir /user/cloudera/input
fi

	hadoop fs -copyFromLocal input/divyatrips.txt /home/cloudera/spark-2.1.1-bin-hadoop2.7/input

    	hadoop fs -rm -r -f /user/cloudera/output/an

sbt package
jarfile="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/target/SparkScalaProject.jar
echo $jarfile
cd /home/cloudera/spark-2.1.1-bin-hadoop2.7
./bin/spark-submit --class "cs522.mum.sparkscala.main.MainObject" --master local[4] $jarfile
