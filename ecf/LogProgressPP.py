import sys
lib = "%EXP_DIR%"
sys.path.insert(0, lib)
import scheduler
import json
import os
import subprocess

print("ECF_JOB_CMD: %ECF_JOB_CMD%")
print(scheduler.__file__)

exp_name = "%EXP%"
server_logfile = "%SERVER_LOGFILE%"
stream = "%STREAM%"
if stream == "":
    stream = None

server = scheduler.EcflowServerFromFile(lib + "/Env_server", server_logfile)

ecf_name = "%ECF_NAME%"
ecf_pass = "%ECF_PASS%"
ecf_tryno = "%ECF_TRYNO%"
ecf_rid = "%ECF_RID%"
next_dtg = "%DTG_NEXT%"
submission_id = "%SUBMISSION_ID%"

# TODO stream
stream = None

task = scheduler.EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid, submission_id)

# This will also handle call to sys.exit(), i.e. Client.__exit__ will still be called.
with scheduler.EcflowClient(server, task) as ci:

    system = json.load(open(lib + "/exp_system.json", "r"))
    wd = system["0"]["exp_dir"]
    st = ""
    if stream is not None and stream != "":
        st = "_stream_" + stream
    progress_file = wd + "/progressPP" + st + ".json"
    # Update progress
    progress = {"DTGPP": next_dtg}
    json.dump(progress, open(progress_file, "w"), indent=2)

