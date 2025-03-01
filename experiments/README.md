# Experiments

This file outlines how to run the experiments on Ubuntu 20.04 or Ubuntu 22.04 on x86-64 machines. For the following experiments, Hadoop 3.3.6, Hive 2.3.9, and a custom version of Spark 3.4 (with Delta Lake 2.4.0 and Iceberg 1.5.2) were set up on a cluster. Yarn scheduler from Hadoop was used to run any spark jobs.

## Set up
Set up Hadoop 3.3.6 (both HDFS and Yarn) on a cluster. Example config files that were used for the experiment are under confgs/hdfs. After the set up, create directories on HDFS as follows:
```bash
hdfs dfs -mkdir /tmp
hdfs dfs -chmod g+w /tmp
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod g+w /user/hive/warehouse
hdfs dfs -mkdir -p /spark_warehouse
hdfs dfs -chmod g+w /spark_warehouse
```

Set up Hive 2.3.9, on a single node backed by MySQL. 
Example hive config file used for the experiment is under configs/hive  
```bash
sudo apt install mysql-server
sudo mysql
CREATE USER 'hive'@'localhost' IDENTIFIED BY 'hive';
GRANT ALL PRIVILEGES ON *.* TO 'hive'@'localhost';
FLUSH PRIVILEGES;
quit
schematool -dbType mysql -initSchema
```

Then ,start running Hive Metastore in background using tmux or screen:
```bash
hive --service metastore
```

On a single node that will run TreeCat server, download the TreeCat repo to build TreeCat: 
```bash
git clone git@github.com:umddb/treecat.git
cd thirdparty
./install.sh
cd ../src
make
```

On the same node, clone spark 3.4 git repo and apply the git patch:
```bash
git clone --branch branch-3.4 --single-branch git@github.com:apache/spark.git
cd spark
git checkout 64c26b7
git apply <treecat_path>/experiments/spark.3.4.patch
```

Build the custom spark distribution 
```bash
./dev/make-distribution.sh --pip --tgz -Phive -Phive-thriftserver -Pyarn
```

Untar the created tarball and fix the spark config files as appropriate. Example spark config files are under confgs/spark. Retar the spark distribution and copy to all the nodes that will serve as client. Now the experiments can be run. 

### Concurrency Experiment 1
This experiment runs a microbenchmark derived from TPC-DS to evaluate the performance of different concurrency control mechanisms. The experiment tests OSPL(optimistic scan range and precision locking), OSL (optimistic scan range locking), and MGL (multiple granularity locking). 

First, generate the data and load to Hive table, by running populate.py with dstreeconfig.json on pyspark under concurrency directory. Then, extract the metadata of the generated data from Hive in json format (named dstree0.json) by running HMSExtractor:
```bash
spark-class org.apache.spark.sql.hive.HMSExtractor dstree0 dstree0.json dstreeconfig.json 100T
```

Now, start running TreeCat on a single node and load the metadata. First compile the executable under experiments directory
```bash
make
```

Next, change the config file for TreeCat. An example config file is under configs/treecat. The configs are as follows:
* backend.txn_mgr_version : The type of concurrency control mechanism. 1 is OSPL, 2 is OSL, and 3 is MGL.
* backend.exec_buf_size : The output buffer size of execution node. 
* txnmgrv1.num_partitions : Size of thread pool for validation of transactions in OSPL. 
* txnmgrv2.num_partitions : Size of thread pool for validation of transactions in OSL. 
* grpc.server_address : The bind address and port number for the grpc server.
* grpc.thread_pool_size : THe size of the grpc thread pool.
* backend.db_path : Path of the backend rocksDB instance.
* backend.compression : The type of compression used for network communcation on top of grpc. We only support snappy. Data is not comprssed if the field is not specified. 
* version_output : An output file to which runtree writes the version of the concurrency control mechanism. 

Now, load the metadata and run TreeCat on a single node:
```bash
./runtree <config_file> dstree0.json
```

Then, we set the experimental configuration, using a json file. Example template is under concurrency/workload.json. The configs are as follows:
* "totalNumThreads" : Number of total clien threads
* "numThreads": Number of client threads run on the particular node,
* "workloadRatio" : Relative percentages of different types of workloads. (optimize:fact table insert:dimension table insert:delete:read).
* "version" : The type of concurrency control mechanism. 1 is OSPL, 2 is OSL, and 3 is MGL. This has to be consistent with the TreeCat config file for correct result output.
* "dryRunTime" : The dry runtime in HH:MM:SS format.
* "experimentTime" : The actual experiment runtime in HH:MM:SS format.
* "scaleFactor" : The corresponding scale factor in TPC-DS. We use 100T, which stands for 100 terabytes.
* "treeAddress" : GRPC address of the TreeCat server.
* "summaryOutput" : Output path to which summary data is appended.
* "opOutput" : Output path to which stats about individual data operation is appended. 

Note that if total number of client threads is not evenly divisble by number of nodes, "numThreads" will have to be manually configured for each client machine. After the configuration file is saved on each client machine. Run the experiment on each node in parallel using utility such as parallel-ssh. Make sure to start experiment only after "Finished loading" is printed on the server node. 
```bash
./spark-class org.apache.spark.exp.ConcurrencyExperiment dstree0.json workload.json
```

After a single run. Kill the TreeCat server, delete the rocksDB directory, specified by backend.db_path and repeat the above with a different set of configs. After experiments are over, all the generated files will have to be downloaded from the client machines. 

### Concurrency Experiment 2
This is a variation of the first experiment, except that we test with multiple databases. For each database created on Hive, (dstree0, dstree1, dstree2, etc. specified by dstreeconfig.json), extract the metadata using HMSExtractor util:
```bash
spark-class org.apache.spark.sql.hive.HMSExtractor <db_name> <db_name>.json dstreeconfig.json 100T
```

Next, concatenadate the extract the metadata to create a single metadata file for 1 database (dstree0.json), 2 databases (dstree0.json and dstree1.json), etc. named 1dstree.json, 2dstree.json etc. The following steps are identical to the above except that we specify another parameter in workload.json:
* "dbDist" : Specifies the distribution of client threads on the particular client node among different databases, delimited by :

For example, "dbDist" of "2:4" would allocate 2 client threads to dstree0 and 4 client threads to the dstree1 for the particular client machine. Again, to balance out the number of client threads among different databases, workload.json file would have to be different for each machine. 

### Scan Range Experiment
This experiment is for comapring read performance of TreeCat with that of DeltaLake, HMS, and Iceberg. Using the config under configs/spark, set the number of files (NUM_FILES) in the scripts that starts with "populate" and run them to generate data for all 4 catalog types. Make sure  spark.sql.catalog.hive_prod, spark.sql.catalog.hive_prod, and spark.sql.defaultCatalog fields in spark-defaults.conf are commented out when populating catalogs other than Iceberg. Also make sure to run populatehms.py before populateiceberg.py as Iceberg is populated by copying metadata from HMS table. The reason is that Iceberg ignores spark.sql.files.maxRecordsPerFile, which is necessary to control the number of files generated per number of records. After populating all the tables, extract the metadata from Hive to load to TreeCat.
```bash
spark-class org.apache.spark.sql.hive.HMSExtractor <db_name> <db_name>.json scanrangeconfig.json 
```

Appropriately change scanrangeconfig.json. The config parameters are as follows:
* "numFiles" : Number of files in the table.
* "partitionRange" : Range of the partition predicate.
* "numCores" : Number of cores used by the client machine.
* "experimentIters" : Number of iterations for the experiment.
* "resultOutput" : The outputfile to which the data is generated.
* "treeAddress" : The GRPC address and port for TreeCat. 

Run the experiment as follows: 
```bash
./spark-class org.apache.spark.exp.ScanRangeExperiment scanrangeconfig.json <catalog_type>
```

For the catalog type, phms stands for HMS, delta stands for Delta Lake, iceberg stands for Iceberg, ptree stands for TreeCat. When running the experiment on TreeCat, repeat the same steps as above to start running TreeCat first. 

For testing core count, use task set utility. For correct result collection, make sure, "numCores" in the config file matches the actual number of cores specified on taskset:
```bash
taskset -c 0-2 ./spark-class org.apache.spark.exp.ScanRangeExperiment scanrangeconfig.json <catalog_type>
```

In between runs for testing different number of files, manually delete the databases using hdfs commands and reset HMS, rather than using spark sql, as metadata files are not properly deleted for Iceberg tables. 

For example, stop the HMS server and on that node, run the following:
```bash
hdfs dfs -rm -r /spark_warehouse/scanhms.db
hdfs dfs -rm -r /spark_warehouse/scandelta.db
hdfs dfs -rm -r /spark_warehouse/scaniceberg.db
hdfs dfs -rm -r /spark_warehouse/scantree.db
sudo mysql
DROP database metastore;
exit
schematool -dbType mysql -initSchema
hive --service metastore
```

### Commit Experiment
This experiment simply measures the relative cost of commit protocol for each catalog type, by measuring the latency of alterTable() operation. We create store sales tables, like above, but do not populate with any data. Simply run the scripts that starts with "create" under commit/ using pyspark to create the tables. Just like above, make sure  spark.sql.catalog.hive_prod, spark.sql.catalog.hive_prod, and spark.sql.defaultCatalog fields in spark-defaults.conf are commented out when populating catalogs other than Iceberg. After creating all the tables, extract the metadata from Hive to load to TreeCat.
```bash
spark-class org.apache.spark.sql.hive.HMSExtractor <db_name> <db_name>.json scanrangeconfig.json 
```

Appropriately change commitconfig.json. The config parameters are as follows:
* "experimentIters" : Number of iterations alterTable is called. Latency of each invocation is measured. 
* "experimentTime" : Duration for testing the total throughput.
* "latencyOutput" : The outputfile to which the latency data is generated.
* "summaryOutput" : The outputfile to which the summary data is generated.
* "treeAddress" : The GRPC address and port for TreeCat. 

Run the experiment as follows: 
```bash
./spark-class org.apache.spark.exp.CommitExperiment commitconfig.json <catalog_type>
```

For the catalog type, hms stands for HMS, delta stands for Delta Lake, iceberg stands for Iceberg, tree stands for TreeCat.
When running TreeCat, make sure the server instance is running before executing the experiment, just like above experiments. 



















backend.txn_mgr_version 1
backend.exec_buf_size 32000
txnmgrv1.num_partitions 5 
txnmgrv2.num_partitions 5
grpc.server_address 0.0.0.0:9876
grpc.thread_pool_size 16
backend.thread_pool_size 100
backend.compression snappy
version_output /tmp/version.txt







However, before the 
























