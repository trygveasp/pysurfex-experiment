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
submission_id = "%SUBMISSION_ID%"
task = scheduler.EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid, submission_id)

# This will also handle call to sys.exit(), i.e. Client.__exit__ will still be called.
with scheduler.EcflowClient(server, task) as ci:

    def init_run(system, paths_to_sync, stream_nr=None):

        rev = paths_to_sync["revision"]
        pysurfex_experiment = paths_to_sync["pysurfex_experiment"]
        offline_source = paths_to_sync["offline_source"]
        pysurfex = paths_to_sync["pysurfex"]
        experiment_is_locked_file = paths_to_sync["experiment_is_locked"]
        wd = paths_to_sync["exp_dir"]

        if stream_nr is None:
            stream_nr = ""
        else:
            stream_nr = str(stream_nr)

        experiment_is_locked_file = experiment_is_locked_file.replace("@STREAM@", stream_nr)
        if os.path.exists(experiment_is_locked_file):
            experiment_is_locked = True
        else:
            experiment_is_locked = False

        rsync = system["0"]["RSYNC"].replace("@STREAM@", stream_nr)
        lib0 = system["0"]["SFX_EXP_LIB"].replace("@STREAM@", stream_nr)
        host_name0 = ""

        # Sync pysurfex_experiment to LIB0
        if not experiment_is_locked:
            dirs = ["experiment", "nam", "toml", "config", "ecf", "experiment_tasks", "experiment_scheduler",
                    "experiment_setup"]
            for d in dirs:
                os.makedirs(lib0 + "/" + d, exist_ok=True)
                cmd = rsync + " " + pysurfex_experiment + "/" + d + " " + host_name0 + lib0 + "/" + d + \
                              " --exclude=.git --exclude=__pycache__ --exclude='*.pyc'"
                print(cmd)
                ret = subprocess.call(cmd, shell=True)
                if ret != 0:
                    raise Exception(cmd + " failed!")
        else:
            print("Not resyncing " + pysurfex_experiment + " as experiment is locked")

        # Sync pysurfex to LIB0
        if not experiment_is_locked:
            dirs = ["surfex"]
            for d in dirs:
                os.makedirs(lib0 + "/" + d, exist_ok=True)
                cmd = rsync + " " + pysurfex + "/" + d + "/ " + host_name0 + lib0 + "/" + d + \
                              " --exclude=.git --exclude=__pycache__ --exclude='*.pyc'"
                print(cmd)
                ret = subprocess.call(cmd, shell=True)
                if ret != 0:
                    raise Exception(cmd + " failed!")
        else:
            print("Not resyncing " + pysurfex_experiment + " as experiment is locked")

        # Sync REV to LIB0
        if not experiment_is_locked:
            if rev != wd:
                cmd = rsync + " " + rev + "/ " + host_name0 + lib0 + \
                      " --exclude=.git --exclude=.idea --exclude=__pycache__ --exclude='*.pyc'"
                print(cmd)
                ret = subprocess.call(cmd, shell=True)
                if ret != 0:
                    raise Exception(cmd + " failed!")
            else:
                print("REV == WD. No syncing needed")
        else:
            print("Not resyncing REV as experiment is locked")

        # Sync offline source code to LIB0
        if not experiment_is_locked:
            if offline_source is not None:
                if rev != wd:

                    cmd = rsync + " " + offline_source + "/ " + host_name0 + lib0 + \
                          "/offline --exclude=.git --exclude=.idea --exclude=__pycache__ --exclude='*.pyc'"
                    print(cmd)
                    ret = subprocess.call(cmd, shell=True)
                    if ret != 0:
                        raise Exception(cmd + " failed!")
                else:
                    print("REV == WD. No syncing needed")
        else:
            print("Not resyncing REV as experiment is locked")

        # Sync WD to LIB
        # Always sync WD unless it is not same as SFX_EXP_LIB
        if wd != lib0:
            cmd = rsync + " " + wd + "/ " + host_name0 + lib0 + \
                  " --exclude=.git --exclude=.idea --exclude=__pycache__ --exclude='*.pyc'"
            print(cmd)
            ret = subprocess.call(cmd, shell=True)
            if ret != 0:
                raise Exception

        # TODO sync LIB to stream

        host_label = []
        for h in system:
            host_label.append(system[h]["HOSTNAME"])

        # Sync HM_LIB beween hosts
        # TODO sync streams
        if len(host_label) > 1:
            for host in range(1, len(host_label)):
                host = str(host)
                print("Syncing to HOST" + host + " with label " + host_label[int(host)])
                rsync = system[host]["RSYNC"].replace("@STREAM@", stream_nr)
                libn = system[host]["SFX_EXP_LIB"].replace("@STREAM@", stream_nr)
                datan = system[host]["SFX_EXP_DATA"].replace("@STREAM@", stream_nr)
                mkdirn = system[host]["MKDIR"].replace("@STREAM@", stream_nr)
                host_namen = system[host]["LOGIN_HOST"].replace("@STREAM@", stream_nr)
                sync_data = system[host]["SYNC_DATA"]

                if sync_data:
                    # libn = system.get_var("SFX_EXP_LIB", host, stream=stream)
                    # datan = system.get_var("SFX_EXP_DATA", host, stream=stream)
                    # mkdirn = system.get_var("MKDIR", host, stream=stream)
                    # host_namen = system.get_var("HOST_NAME", host, stream=stream)
                    ssh = ""
                    if host_namen != "":
                        ssh = "ssh " + os.environ["USER"] + "@" + host_namen
                        host_namen = os.environ["USER"] + "@" + host_namen + ":"

                    cmd = mkdirn + " " + datan
                    print(cmd)
                    ret = subprocess.call(cmd, shell=True)
                    if ret != 0:
                        raise Exception
                    cmd = mkdirn + " " + libn
                    if ssh != "":
                        cmd = ssh + " \"" + mkdirn + " " + libn + "\""
                    print(cmd)
                    subprocess.call(cmd, shell=True)
                    if ret != 0:
                        raise Exception
                    cmd = rsync + " " + host_name0 + lib0 + "/ " + host_namen + libn + \
                                  " --exclude=.git --exclude=.idea --exclude=__pycache__ --exclude='*.pyc'"
                    print(cmd)
                    subprocess.call(cmd, shell=True)
                    if ret != 0:
                        raise Exception
                else:
                    print("Data sync to " + host_namen + " disabled")

        print("Lock experiment")
        fh = open(experiment_is_locked_file, "w")
        fh.write("Something from git?")
        fh.close()
        print("Finished syncing")


    # InitRun always runs from HOST0
    system2 = json.load(open(lib + "/exp_system_vars.json", "r"))

    paths_to_sync2 = json.load(open(lib + "/paths_to_sync.json", "r"))

    init_run(system2, paths_to_sync2, stream_nr=stream)
