import json
from pathlib import Path

def save_irace_metadata(date_ref, path, params: dict):
    irace_metadata = {
            "instance": params['instance'], 
            "instance_id": params['instance_id'],
            "configuration_id": params['configuration_id'], 
            "parameters": params['parameters'], 
            "begin": str(params['begin']), 
            "end": str(params['end']), 
            "total": str(params['total']), 
            "execution_status": params['execution_status']
        }

    file = open(f'{path}/irace-metadata-{date_ref}.txt', 'a')
    file.write(json.dumps(irace_metadata))
    file.write('\n')
    file.close()