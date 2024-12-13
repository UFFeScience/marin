from pyspark.sql import SparkSession

def build_session(parameters: dict):

    spark = SparkSession.builder\
                .master("local")\
                .appName('Irace')\
                .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0")\
                .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")\
                .config("spark.executor.memory", parameters.get('executorMemory', "1G"))\
                .config("spark.executor.instances", parameters.get('executorInstances', 2))\
                .config("spark.executor.cores", parameters.get('driverCores', 1))\
                .config("spark.sql.shuffle.partitions", parameters.get('sqlShufflePartitions', 200))\
                .config("spark.default.parallelism", parameters.get('defaultParallelism', 8))\
                .config("spark.storage.memoryFraction", parameters.get('memoryFraction', 0.6))\
                .config("spark.shuffle.compress", parameters.get('shuffleCompress', True))\
                .config("spark.sql.sources.partitionOverwriteMode", parameters.get('partitionOverwriteMode', 'static'))\
                .config("spark.cassandra.output.consistency.level", parameters.get('cassandraOutputConsistencyLevel', 'LOCAL_ONE'))\
                .config("spark.cassandra.input.split.sizeInMB", parameters.get('cassandraInputSplitSizeinMB', 64))\
                .config("spark.cassandra.output.batch.size.rows", parameters.get('cassandraOutputBatchSizeRows', 'auto'))\
                .config("spark.cassandra.output.batch.grouping.buffer.size", parameters.get('cassandraOutputBatchGroupingBufferSize', 1000))\
                .config("spark.cassandra.output.concurrent.writes", parameters.get('cassandraOutputConcurrentWrites', 5))\
                .getOrCreate()
    
    return spark