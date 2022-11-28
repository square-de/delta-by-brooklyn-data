## General Information

These benchmarks were run with AWS EMR version 6.8, Delta 2.1.0, and Iceberg 0.14.1.
Modifications to the Delta and Iceberg versions can be made in `run-benchmark.py`.
Modifications to the EMR version may require a change to
the `iceberg_artifact_name` in `benchmarks.py`.

## Setting up the infrastructure

Our infrastructure for these benchmarks was created through the AWS CLI,
largely following the manual steps listed in the [primary README file](../../../../../README.md)
For convenience when creating an EMR cluster,
we added the following to our `~/.zshrc` file.

```bash
user_name=""
key_name=""
aws_profile=""
workers=16
emr_version="6.8.0"
metastore_jdbc=""
metastore_user=""
metastore_pswd=""
region=""
subnet_id=""
coordinator_sg=""
worker_sg=""

function build-emr-cluster {

  delta_cluster_id=$(aws emr create-cluster \
    --profile $aws_profile --region ${region} \
    --name "delta_performance_benchmarks_cluster - CLI Build" \
    --release-label emr-${emr_version} --log-uri 's3n://your-log-bucket/' \
    --tags "Owner=${user_name}" --applications Name=Ganglia Name=Spark Name=Hive \
    --ec2-attributes '{"KeyName":"'${key_name}'","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"'${subnet_id}'","EmrManagedSlaveSecurityGroup":"'${worker_sg}'","EmrManagedMasterSecurityGroup":"'${coordinator_sg}'"}' \
    --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"i3.2xlarge","Name":"Master - 1"},{"InstanceCount":'${workers}',"InstanceGroupType":"CORE","InstanceType":"i3.2xlarge","Name":"Core - '${workers}'"}]' \
    --configurations '[{"Classification":"spark-defaults","Properties":{"spark.driver.memory":"18971M","spark.driver.maxResultSize":"5692M","spark.sql.crossJoin.enabled":"true","spark.sql.broadcastTimeout":"7200","spark.sql.debug.maxToStringFields":"1000"}},
                      {"Classification":"hive-site","Properties":{"javax.jdo.option.ConnectionUserName":"'${metastore_user}'","javax.jdo.option.ConnectionDriverName":"org.mariadb.jdbc.Driver","javax.jdo.option.ConnectionPassword":"'${metastore_pswd}'","javax.jdo.option.ConnectionURL":"'${metastore_jdbc}'"}},
                      {"Classification":"livy-conf","Properties":{"livy.server.session.timeout-check":"true","livy.server.session.timeout":"24h","livy.server.yarn.app-lookup-timeout":"600s"}}]' \
    --auto-scaling-role EMR_AutoScaling_DefaultRole --service-role EMR_DefaultRole \
    --ebs-root-volume-size 100 --enable-debugging \
  | jq -r '.ClusterId') && echo $delta_cluster_id
  aws emr wait cluster-running --profile $aws_profile --region ${region} --cluster-id $delta_cluster_id  && \
  delta_cluster=$(aws --profile $aws_profile --region ${region} emr describe-cluster --cluster-id $delta_cluster_id | jq -r .Cluster.MasterPublicDnsName) && echo $delta_cluster

}
```

## Parquet Prep

The process here is similar to the one listed in the
[primary README file](../../../../../README.md):

```bash
./run-benchmark.py \
    --cluster-hostname <HOSTNAME> \
    -i <PEM_FILE> \
    --ssh-user <SSH_USER> \
    --benchmark-path <BENCHMARK_PATH> \
    --source-path <SOURCE_PATH> \
    --cloud-provider <CLOUD_PROVIDER> \
    --benchmark etl-1tb-parquet-prep
```

The discrepency being on the benchmark itself. To prep the data for use,
run the command above, replacing `<SOURCE_PATH>` with the full path to your the 1tb datset.
We used the data available in `s3://devrel-delta-datasets/tpcds-2.13/tpcds_sf1000_parquet/`
for these benchmarks. `<BENCHMARK_PATH>` will hold the path to your s3 bucket
where you would like the results to land.

## Running the Benchmarks

Once the data prep is complete, the benchmarks can be run following the command structure above:

```bash
./run-benchmark.py \
    --cluster-hostname <HOSTNAME> \
    -i <PEM_FILE> \
    --ssh-user <SSH_USER> \
    --benchmark-path <BENCHMARK_PATH> \
    --source-path <SOURCE_PATH> \
    --cloud-provider <CLOUD_PROVIDER> \
    --benchmark etl-1tb-parquet-prep
```

The `<BENCHMARK_PATH>` location from the data prep will become your source,
and should look similar to
`s3://your-bucket/databases/etlprep_sf1000_parquet_{timestamp}_etl_1tb_parquet_prep/`.

`<SOURCE_PATH>` can be changed to be specific to Iceberg or Delta, or a separate bucket for each.

Iceberg Command:

```bash
./run-benchmark.py \
    --cluster-hostname <HOSTNAME> \
    -i <PEM_FILE> \
    --ssh-user <SSH_USER> \
    --benchmark-path <BENCHMARK_PATH> \
    --source-path <SOURCE_PATH> \
    --cloud-provider <CLOUD_PROVIDER> \
    --benchmark etl-1tb-iceberg
```

Delta Command:

```bash
./run-benchmark.py \
    --cluster-hostname <HOSTNAME> \
    -i <PEM_FILE> \
    --ssh-user <SSH_USER> \
    --benchmark-path <BENCHMARK_PATH> \
    --source-path <SOURCE_PATH> \
    --cloud-provider <CLOUD_PROVIDER> \
    --benchmark etl-1tb-delta
```

## Reading Results

JSON and CSV reports will be generated on completion under `<SOURCE_PATH>/reports/`.
The CSV results from our initial runs are located inside the [csv directory](./results/csv/),
as well as a [concat_results.csv](./results/csv/concat_results.csv) where we have
combined all the results into one long CSV for ease of analysis.

Our results for file counts are output to the primary EMR instance where
[run_benchmarks.py](../../../../../run-benchmark.py) is located
-- they are not currently written out to s3.

To generate the charts presented in our blog post, we utilized duckdb and plotly to
aggregate the results and display them, respectively. Tables are created in
[generate_tables.py](./results/generate_tables.py), and then charts are generated by
[generate_charts.py](./results/generate_charts.py) and are output as PNGs to
[the charts directory](./results/charts/).
