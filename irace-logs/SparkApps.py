import findspark
findspark.init("/opt/spark/")

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, BooleanType
from pyspark.sql.functions import lit, col, udf, split
from helpers.spark_session import build_session
from datetime import datetime
from pathlib import Path
import ipaddress
import uuid
import os
import re

class SparkApps:
    def __init__(self, args:dict = None) -> None:
        self.parameters = args if args is not None else dict()
        self.spark, self.sc = self.spark_connection()
        self.cassandra_key_space = 'main_keyspace'
        self.cassandra_wordcount_table = 'wordcount'
        self.cassandra_metadata_table = 'irace_metadata'
        self.cassandra_logs_table = 'logs'
        self.cassandra_logs_agg_table = 'logs_agg'
        self.cassandra_logs_wordcount = 'logs_wordcount'

        self.extract_ip_network_udf = udf(lambda ip: self.extract_ip_network(ip), StringType())
        self.extract_ip_version_udf = udf(lambda ip: self.extract_ip_version(ip), StringType())
        self.extract_ip_broadcast_udf = udf(lambda ip: self.extract_ip_broadcast(ip), StringType())
        self.extract_ip_netaddress_udf = udf(lambda ip: self.extract_ip_netaddress(ip), StringType())
        self.extract_ip_private_udf = udf(lambda ip: self.extract_ip_private(ip), StringType())
        self.extract_ip_netmask_udf = udf(lambda ip: self.extract_ip_netmask(ip), StringType())



    def spark_connection(self):
        try:
            spark = build_session(self.parameters)
            
            sc=spark.sparkContext
            sc.setLogLevel("ERROR")

            return spark, sc  
        
        except Exception:
            print(f'Configuração não permitida, parâmetros: {self.parameters}')
    
    def spark_stop(self):
        self.spark.stop()
    
    def spark_config(self):
        spark_config = self.spark.sparkContext.getConf()

        return spark_config.getAll()
    
    @staticmethod
    def build_wordcount_schema():
        schema = StructType([ \
                            StructField("word",StringType(),True), \
                            StructField("count",IntegerType(),True)
                        ])        
        return schema
    
    @staticmethod
    def build_metadata_schema():
        schema = StructType([ \
                            StructField("execution_id",IntegerType(),True), \
                            StructField("instance_id",IntegerType(),True), \
                            StructField("configuration_id",IntegerType(),True), \
                            StructField("parameters",StringType(),True), \
                            StructField("begin_timestamp",TimestampType(),True), \
                            StructField("end_timestamp",TimestampType(),True), \
                            StructField("total_time_seconds",FloatType(),True), \
                            StructField("execution_status",BooleanType(),True)
                        ])        
        return schema
    
    def list_files(self, path):        
        files = []
        dir = os.listdir(path) 

        for file in dir:
            files.append(file)

        return files
        
    def save_data_cassandra(self, df, keyspace, table, mode):
        df.write.format("org.apache.spark.sql.cassandra")\
              .option("confirm.truncate","true") \
              .options(table=table, keyspace=keyspace) \
              .mode(mode) \
              .save()
        
    def load_data_from_cassandra(self, key_space, table):
        df = self.spark.read.format("org.apache.spark.sql.cassandra") \
            .options(table=table, keyspace=key_space)\
            .load()
        
        return df
    
    def irace_save_metadata(self, execution_id, instance_id, configuration_id, parameters, begin, end, total, execution_status):
        
        schema = self.build_metadata_schema()
        data = [(int(execution_id), int(instance_id), int(configuration_id), parameters, begin, end, total, execution_status)]

        df = self.spark.createDataFrame(data, schema=schema)

        uuid_udf = udf(lambda : str(uuid.uuid4()), StringType())

        df = df.withColumn('id', uuid_udf())

        df = df.select('id', 'execution_id', 'instance_id', 
                       'configuration_id', 'parameters', 
                       'begin_timestamp', 'end_timestamp', 'total_time_seconds', 'execution_status')
    

        self.save_data_cassandra(df, self.cassandra_key_space, self.cassandra_metadata_table, 'overwrite')
    
    def word_count(self, path):
        files = self.list_files(path)
        schema = self.build_wordcount_schema()
        df = None

        for file in files:
            text_file = self.sc.textFile(f"{path}/{file}")

            counts = text_file.flatMap(lambda line: line.split(" ")) \
                              .map(lambda word: (word.lower(), 1)) \
                              .reduceByKey(lambda x, y: x + y)
        
            df_count = self.spark.createDataFrame(counts, schema=schema)
            df_count = df_count.withColumn('source', lit(file))

            df_count = df_count.dropna()
            df_count = df_count.filter(col('word')!='')

            if df is None:
                df = df_count
            else:
                df = df.union(df_count)

        self.save_data_cassandra(df, self.cassandra_key_space, self.cassandra_wordcount_table, 'overwrite')
    
    @staticmethod
    def extract_ip(line):
        ip_pattern = r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
        match = re.search(ip_pattern, line)
        if match:
            return match.group()
        else:
            return None

    def extract_ip_network(self, ip):
        return ipaddress.ip_network(ip)
    
    def extract_ip_version(self, ip):
        return ipaddress.ip_address(ip).version
     
    def extract_ip_private(self, ip):
        return ipaddress.ip_address(ip).is_private
    
    def extract_ip_broadcast(self, ip):
        return ipaddress.ip_network(ip).broadcast_address
    
    def extract_ip_netaddress(self, ip):
        return ipaddress.ip_network(ip).network_address
    
    def extract_ip_netmask(self, ip):
        return ipaddress.ip_network(ip).netmask
    
    @staticmethod
    def extract_info(ip):
        ip_obj = ipaddress.ip_address(ip)
        network = ipaddress.ip_network(ip)
        version = ip_obj.version
        is_private = ip_obj.is_private
        broadcast = str(network.broadcast_address)
        net_address = str(network.network_address)
        subnet_mask = str(network.netmask)

        return ip, str(network), str(version), str(is_private), broadcast, net_address, subnet_mask

    def construct_dataset(self, df):

        df = df.drop('count')

        schema = StructType([
            StructField("ip", StringType(), True),
            StructField("network", StringType(), True),
            StructField("version", StringType(), True),
            StructField("is_private", StringType(), True),
            StructField("broadcast", StringType(), True),
            StructField("network_address", StringType(), True),
            StructField("subnet_mask", StringType(), True)
        ])

        extract_info_udf = udf(lambda ip: SparkApps.extract_info(ip), schema)

        df = df.withColumn("ip_info", extract_info_udf(df["word"]))

        df = df.select("ip_info.ip", "ip_info.network", "ip_info.version", "ip_info.is_private",
                       "ip_info.broadcast", "ip_info.network_address", "ip_info.subnet_mask")

        df.show()

        file_path = f'{Path.cwd()}/ip_info'
               
        df.write.csv(file_path, mode="overwrite", header=True)

    def extract_ip_info(self, df):

        df = df.drop('count')

        df = df.withColumn("network", self.extract_ip_network_udf(df['word']))\
               .withColumn("version", self.extract_ip_version_udf(df['word']))\
               .withColumn("is_private", self.extract_ip_private_udf(df['word']))\
               .withColumn("broadcast", self.extract_ip_broadcast_udf(df['word']))\
               .withColumn("net_address", self.extract_ip_netaddress_udf(df['word']))\
               .withColumn("netmask", self.extract_ip_netmask_udf(df['word']))\
               
        file_path = f'{Path.cwd()}/ip_info'
               
        df.write.csv(file_path, mode="overwrite", header=True)


    def logs_word_count(self, path):
        schema = self.build_wordcount_schema()

        text_files = self.sc.textFile(f"{path}/*.txt")

        text_files = text_files.map(self.extract_ip).filter(lambda x: x is not None)   

        counts = text_files.flatMap(lambda line: line.split(" ")) \
                              .map(lambda word: (word.lower(), 1)) \
                              .reduceByKey(lambda x, y: x + y)
        
        df_count = self.spark.createDataFrame(counts, schema=schema)

        self.construct_dataset(df_count)
        #df_count.show(n=5)
        print(datetime.now(), 'Ingesting data into cassandra table')
        
        self.save_data_cassandra(df_count, self.cassandra_key_space, self.cassandra_logs_wordcount, 'append')

        end = datetime.now()

        print(counts.toDebugString()) 
        print(text_files.toDebugString()) 

        print(end, 'Data ingested succefully in cassandra table')

        return end        

    def logs(self, path):
        files = self.list_files(path)

        for file in files:
            print(datetime.now(), file)

            df = self.spark.read.csv(f"{path}/{file}", sep=' ', header=False)
            
            df = df.withColumn('ip', split(col('_c2'), '\t').getItem(0))
            df = df.withColumn('time', col('_c4'))

            df = df.select(col('ip'), col('time'))

            self.save_data_cassandra(df, self.cassandra_key_space, self.cassandra_logs_table, 'append')

    def agg_log_data(self):

        print(datetime.now(), 'Data aggregation')

        df = self.spark.read.format("org.apache.spark.sql.cassandra").options(table=self.cassandra_logs_table, keyspace=self.cassandra_key_space).load()

        df = df.groupBy('ip').agg({'ip': 'count'})

        df = df.withColumnRenamed('count(ip)', 'count')

        df = df.select('ip', 'count')

        self.save_data_cassandra(df, self.cassandra_key_space, self.cassandra_logs_agg_table, 'overwrite')

    def read_and_filter_data(self):

        df = self.spark.read.format("org.apache.spark.sql.cassandra") \
                            .options(table=self.cassandra_logs_wordcount, keyspace=self.cassandra_key_space) \
                            .load()
        
        df_filtered = df.filter(col("word").startswith("44."))

        total = df_filtered.count()

        return total
