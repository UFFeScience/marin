import argparse
from datetime import datetime

def build_parser():

    parser = argparse.ArgumentParser()
    parser.add_argument('--instanceId', type=str, help='Value created by Irace')
    parser.add_argument('--instance', type=str, help='One of the instances listed on instance file')
    parser.add_argument('--configId', type=str, help='Value created by Irace')
    parser.add_argument('--seed', type=int, help='Value created by Irace - defines the number used to sort a set of configurations to be considered in a iteration')
    parser.add_argument('--executorMemory', type=int, help='Spark session parameter')
    parser.add_argument('--executorInstances', type=int, help='Spark session parameter')
    parser.add_argument('--driverCores', type=int, help='Spark session parameter')
    parser.add_argument('--sqlShufflePartitions', type=int, help='Spark session parameter')
    parser.add_argument('--defaultParallelism', type=int, help='Spark session parameter')
    parser.add_argument('--memoryFraction', type=float, help='Spark session parameter')
    parser.add_argument('--shuffleCompress', type=bool, help='Spark session parameter')
    parser.add_argument('--partitionOverwriteMode', type=str, help='Spark session parameter')
    parser.add_argument('--cassandraOutputConsistencyLevel', type=str, help='Spark session parameter')
    parser.add_argument('--cassandraInputSplitSizeinMB', type=int, help='Spark session parameter')
    parser.add_argument('--cassandraOutputBatchSizeRows', type=str, help='Spark session parameter')
    parser.add_argument('--cassandraOutputBatchGroupingBufferSize', type=int, help='Spark session parameter')
    parser.add_argument('--cassandraOutputConcurrentWrites', type=int, help='Spark session parameter')

    return parser
