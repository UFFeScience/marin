from helpers.irace_metadata import save_irace_metadata
from datetime import datetime, timedelta
from helpers.parser import build_parser
from SparkApps import SparkApps
from pathlib import Path
import sys

bash_parameters = sys.argv

args = build_parser().parse_args()

date_ref = (datetime.now() - timedelta(hours=3)).date().strftime("%Y-%m-%d")

print(datetime.now(), 'Starting process')

configuration_id = args.configId
instance_id = args.instanceId
seed = args.seed
instance = args.instance

file_path = f'{Path.cwd()}/files/{instance}'

# creating set of parameters to build spark session
parameters = {}

for key, value in vars(args).items():
    if value != None:
        if key == 'executorMemory':
            parameters[key] = str(value) + 'G'
        else:
            parameters[key] = value

spark_obj = SparkApps(parameters)

# running spark application
try:
    begin = datetime.now()
    end = spark_obj.logs_word_count(path=file_path)
    total = (end - begin).total_seconds() 

    execution = True

except Exception as error:
    end = datetime.now()
    execution = False
    print(error)
    # adding arbitrary value high enough to not impact the final result 
    # probably this confguration will be discarded when Irace is running
    total = 100000000


# Building dict to save irace metadate on every execution
    
if instance_id is not None:
    # if instance_id is not None it means that Irace is running
    # because this value is definied by Irace

    irace_metadata = {
        'instance': instance,
        'instance_id': instance_id,
        'configuration_id': configuration_id,
        'parameters': parameters,
        'begin': begin, 
        'end': end,
        'total': total,
        'execution_status': execution
    }

    path = f"{Path.cwd()}/logs"

    save_irace_metadata(date_ref, path, irace_metadata)

# DO NOT REMOVE THIS PRINT AND DO NOT PRINT ANYTHING ELSE AFTER THIS
# THE VALUE WILL BE USE BY IRACE 
#####################################################################

print(total)

######################################################################

spark_obj.spark_stop()
