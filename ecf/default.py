import sys
import json
lib = "%LIB%"
host = "@HOST_TO_BE_SUBSTITUTED@"

system_variables = json.load(open(lib + "/exp_system_vars.json", "r"))[host]
scheduler_pythonpath = system_variables["SCHEDULER_PYTHONPATH"]
if scheduler_pythonpath != "":
    for sp in scheduler_pythonpath.split(":"):
        sys.path.insert(0, sp)

print(lib)
sys.path.insert(0, lib)
import scheduler
import experiment_tasks
import inspect

wrapper = "@WRAPPER_TO_BE_SUBSTITUTED@"
server_logfile = "%SERVER_LOGFILE%"
exp_name = "%EXP%"
stream = "%STREAM%"
mbr = "%ENSMBR%"
dtg = "%DTG%"
dtgbeg = "%DTGBEG%"

if stream == "":
    stream = None
if mbr == "" or int(mbr) < 0:
    mbr = None

print(sys.path)
print(scheduler.__file__)
server = scheduler.EcflowServerFromFile(lib + "/Env_server", server_logfile)
ecf_name = "%ECF_NAME%"
ecf_pass = "%ECF_PASS%"
ecf_tryno = "%ECF_TRYNO%"
ecf_rid = "%ECF_RID%"
submission_id = "%SUBMISSION_ID%"
task_name = "%TASK%"
args = "%ARGS%"
if args == "":
    args = None

task = scheduler.EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid, submission_id)

# This will also handle call to sys.exit(), i.e. Client.__exit__ will still be called.
with scheduler.EcflowClient(server, task) as ci:

    # Dummy commands to try out your self
    print("PYTHONPATH=" + scheduler_pythonpath + " && %EXP_DIR%/bin/ECF_status_exp %EXP_DIR%/scheduler.json " +
          "%ECF_NAME% %ECF_TRYNO% %ECF_PASS% -ecf_rid  -submission_id ")
    print("PYTHONPATH=" + scheduler_pythonpath + " && %EXP_DIR%/bin/ECF_kill_exp %EXP_DIR%/scheduler.json " +
          "%ECF_NAME% %ECF_TRYNO% %ECF_PASS% -ecf_rid %ECF_RID% -submission_id ")

    print("Running task " + task_name)
    classes = inspect.getmembers(experiment_tasks, inspect.isclass)
    task_class = None
    for c in classes:
        cname = c[0]
        ctask = c[1]
        if cname == task_name:
            task_class = ctask

    if task_class is None:
        raise Exception("Class not found for task " + task_name)

    # The task knows which host it runs on and which member it is
    task_config = json.load(open(lib + "/exp_configuration.json", "r"))
    progress = {
        "DTG": dtg,
        "DTGBEG": dtgbeg,
        "DTGPP": dtg
    }

    system_file_paths = json.load(open(lib + "/exp_system.json", "r"))[host]
    task_class(task, task_config, system_variables, system_file_paths, progress, mbr=mbr, stream=stream, args=args).run(
        wrapper=wrapper)
