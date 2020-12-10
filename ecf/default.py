import scheduler
import experiment
import os
import sys

host = "@HOST_TO_BE_SUBSTITUTED@"
wrapper = "@WRAPPER_TO_BE_SUBSTITUTED@"
lib = "%LIB%"
exp = "%EXP%"
stream = "%STREAM%"
mbr = "%ENSMBR%"
dtg = "%DTG%"
dtgbeg = "%DTGBEG%"

if stream == "":
    stream = None
if mbr == "" or int(mbr) < 0:
    mbr = None

libs = ["surfex", "scheduler", "experiment"]
for ll in libs:
    if os.path.exists(lib + "/" + ll):
        sys.path.insert(0, lib + "/" + ll)

progress = {"DTG": dtg, "DTGBEG": dtgbeg}
progress_pp = {"DTGPP": dtg}
progress = experiment.Progress(progress, progress_pp)
exp = experiment.ExpFromFiles(exp, lib, host=host, progress=progress)
server = exp.server

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

# Dummy commands to try out your self
print("%LIB%/bin/ECF_status_exp %EXP% %LIB% %ECF_NAME% %ECF_TRYNO% %ECF_PASS% " +
      "-ecf_rid %ECF_RID% -submission_id %SUBMISSION_ID%")
print("%LIB%/bin/ECF_kill_exp %EXP% %LIB% %ECF_NAME% %ECF_TRYNO% %ECF_PASS% " +
      "-ecf_rid %ECF_RID% -submission_id %SUBMISSION_ID%")

# This will also handle call to sys.exit(), i.e. Client.__exit__ will still be called.
with scheduler.EcflowClient(server, task) as ci:
    print("Running task " + task_name)
    task_class = getattr(experiment.tasks, task_name)

    task_class(task, exp, host=host, mbr=mbr, stream=stream, args=args).run(wrapper=wrapper)
