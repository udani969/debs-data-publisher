#!/bin/sh
echo "Running publisher $6 times"
for i in `seq 1 $6`
  do
    nohup  java -jar target/debs-data-publisher-1.0.0-jar-with-dependencies.jar $1 $2 $3 $4 $5 > /dev/null 2>&1 &
 done