import base64
import json
import os
import zlib
import re

import pandas as pd

def extract_logs_to_df(data):
    """Helper function to extract the logs from a loaded dict holding the json data from .ndjson experimental files."""

    # Go over resources and extract logs.
    # Extract logs with id, start timestamp.
    logs = []

    for response in data['detailed_job_stats']['responses']:
        for resource in response['resources']:
            if resource.get('type') == 1:
                log = resource['payload']

                bytes_data = base64.b64decode(log)
                log = zlib.decompress(bytes_data).decode()

                # extract from log Lambda number (to save log to)
                lam_no = int(re.findall(r"Lambda (\d+) writing to", log)[0])

                request_id = response['container']['requestId']

                logs.append({'requestId': request_id, 'lam_no': lam_no, 'log':log})

        # Recursive log extract from self-invocations
        if 'invokedResponses' in response.keys():
            for self_response in response['invokedResponses']:
                for resource in self_response['resources']:
                    if resource.get('type') == 1:
                        log = resource['payload']

                        bytes_data = base64.b64decode(log)
                        log = zlib.decompress(bytes_data).decode()

                        # extract from log Lambda number (to save log to).
                        # For these messages, this is likely not found directly.
                        try:
                            lam_no = int(re.findall(r"Lambda (\d+) writing to", log)[0])
                        except:

                            if "settings from message are different, reinitialized with:" in log:
                                needle = "settings from message are different, reinitialized with:"
                            else:
                                needle = "current worker settings: "

                            lines = log.splitlines()
                            line = list(filter(lambda line: needle in line, lines))[0]
                            raw_info = line[line.find(needle) + len(needle):].strip()
                            info = json.loads(raw_info)
                            lam_no = int(os.path.basename(info['spillRootURI'])[3:])

                        request_id = response['container']['requestId']
                        logs.append({'requestId': request_id, 'lam_no': lam_no, 'log':log})
    return pd.DataFrame(logs)