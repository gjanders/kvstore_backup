import subprocess
import requests
from requests.auth import HTTPBasicAuth
import json
import sys
import os
import time
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

from splunklib.modularinput import Argument
import splunklib.modularinput as smi

class KVStoreBackup(smi.Script):
    def get_scheme(self):
        scheme = smi.Scheme("KVStoreBackup")
        scheme.description = "Automated kvstore backups on a single instance"
        scheme.use_external_validation = True
        scheme.streaming_mode_xml = True
        scheme.use_single_instance = False

        return scheme

    def stream_events(self, inputs, ew):        
        # Use the system token for authentication
        token = self.service.token

        # Cleanup backups older than 7 days
        # Define the directory to clean up
        directory = "/opt/splunk/var/lib/splunk/kvstorebackup/"

        # Get the current time
        now = time.time()

        # Define the age threshold (1 days in seconds)
        # lowered to 1 due to the giant size of the kvstore on onesplunk
        age_threshold = 7 * 24 * 60 * 60

        # Traverse the directory
        for root, dirs, files in os.walk(directory):
            for name in files:
                file_path = os.path.join(root, name)
                # Check the age of the file
                if os.stat(file_path).st_mtime < now - age_threshold:
                    os.remove(file_path)
                    event = smi.Event()
                    event.data = f"Removed file: {file_path}"
                    ew.write_event(event)
            for name in dirs:
                dir_path = os.path.join(root, name)
                # Check the age of the directory
                if os.stat(dir_path).st_mtime < now - age_threshold:
                    shutil.rmtree(dir_path)
                    event = smi.Event()
                    event.data = f"Removed directory: {dir_path}"
                    ew.write_event(event)

        # Define the paths
        splunk_bin_path = "/opt/splunk/bin/splunk"
        headers={'Authorization': 'Splunk %s' % token}

        url = 'https://localhost:8089/services/shcluster/captain/info?output_mode=json'
        res = requests.get(url, headers=headers, verify=False)
        if (res.status_code == 503):
            #logger.debug("Non-shcluster / standalone instance, safe to run on this node")
            pass
        elif (res.status_code != requests.codes.ok):
            #logger.fatal("unable to determine if this is a search head cluster or not, this is a bug, URL=%s statuscode=%s reason=%s, response=\"%s\"" % (url, res.status_code, res.reason, res.text))
            print("Fatal error, unable to determine if this is a search head cluster or not, refer to the logs")
            sys.exit(-1)
        elif (res.status_code == 200):
            #We're in a search head cluster, but are we the captain?
            json_dict = json.loads(res.text)
            if json_dict['origin'] != "https://localhost:8089/services/shcluster/captain/info":
                print("Not on the captain, exiting now")
                return
            else:
                print("On the captain node, running")

        url = 'https://localhost:8089/services/kvstore/backup/create'
        res = requests.post(url, headers=headers, verify=False)

        if (res.status_code == 200):
            event = smi.Event()
            event.data = "200 success code. KVStore backup has started"
            ew.write_event(event)   
        else:
            event = smi.Event()
            event.data = f"KVstore backup failure status_code={res.status_code} text={res.text}"
            ew.write_event(event)

if __name__ == "__main__":
    exitcode = KVStoreBackup().run(sys.argv)
    sys.exit(exitcode)

