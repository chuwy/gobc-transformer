#!/bin/sh

set -e

CONFIG_DIR=$(pwd)

# Download Dataflow Runner to localdir
if [ ! -f dataflow-runner ]; then
  wget http://dl.bintray.com/snowplow/snowplow-generic/dataflow_runner_0.3.0_darwin_amd64.zip
  unzip dataflow_runner_0.3.0_darwin_amd64.zip
fi

# Launch cluster
./dataflow-runner up --emr-config cluster.json

# Assemble Spark job
cd ..
sbt assembly

# Copy Spark job to S3
aws s3 cp target/scala-2.11/bgoctransformer-0.1.0-rc1.jar s3://cara-gobc-sample-log/jars/

# Run playbook
./dataflow-runner run --emr-cluster j-1B38PA4E5CKHL --emr-playbook  playbook.json
