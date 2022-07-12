"""Default ecflow container."""
from distutils.command.config import config
import json
import inspect
import logging
import scheduler
import experiment_tasks


def parse_ecflow_vars():
    """Parse the ecflow variables."""
    return {
        "LIB": "%LIB%",
        "HOST": "@HOST_TO_BE_SUBSTITUTED@",
        "SERVER_LOGFILE": "%SERVER_LOGFILE%",
        "WRAPPER": "@WRAPPER_TO_BE_SUBSTITUTED@",
        "ENSMBR": "%ENSMBR%",
        "DTG": "%DTG%",
        "DTGBEG": "%DTGBEG%",
        "stream": "%STREAM%",
        "TASK_NAME": "%TASK%",
        # TODO from ecflow
        "DEBUG": True,
        "FORCE": False,
        "CHECK_EXISTENCE": True,
        "PRINT_NAMELIST": True,
        "STREAM": "%STREAM%",
        "ARGS": "%ARGS%",
        "ECF_NAME": "%ECF_NAME%",
        "ECF_PASS": "%ECF_PASS%",
        "ECF_TRYNO": "%ECF_TRYNO%",
        "ECF_RID": "%ECF_RID%",
        "SUBMISSION_ID": "%SUBMISSION_ID%"
    }
'''
%nopp"
'''

def read_system_vars(lib, host="0"):
    """Read system dict from json file."""
    with open(lib + "/exp_system_vars.json", mode="r", encoding="utf-8") as file_handler:
        return json.load(file_handler)[host]

def read_ecflow_server_file(lib):
    """Read ecflow host settings."""
    with open(lib + "/Env_server", mode="r", encoding="utf-8") as file_handler:
        return json.load(file_handler)

def read_system_file_paths(lib, host="0"):
    """Read system file paths."""
    with open(lib + "/exp_system.json", mode="r", encoding="utf-8") as file_handler:
        return json.load(file_handler)[host]

def read_exp_configuration(lib, ensmbr=None):
    """Read experiment configuration.

    The task knows which host it runs on and which member it is

    """
    with open(lib + "/exp_configuration.json", mode="r", encoding="utf-8") as file_handler:
        if ensmbr is None:
            return json.load(file_handler)
        return json.load(file_handler)[ensmbr]

def default_main(system, server_settings, task_config, system_file_paths, **kwargs):
    """Ecflow container default method."""
    debug = kwargs.get("DEBUG")
    if debug is None:
        debug = False
    if debug:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                            level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    ecf_host = server_settings.get("ECF_HOST")
    ecf_port = server_settings.get("ECF_PORT")
    server_logfile = kwargs.get("SERVER_LOGFILE")
    server = scheduler.EcflowServer(ecf_host, ecf_port, server_logfile)

    ecf_name = kwargs.get("ECF_NAME")
    ecf_pass = kwargs.get("ECF_PASS")
    ecf_tryno  = kwargs.get("ECF_TRYNO")
    ecf_rid = kwargs.get("ECF_RID")
    submission_id = kwargs.get("SUBMISSION_ID")
    task = scheduler.EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid, submission_id)

    stream = kwargs.get("STREAM")
    task_config["GENERAL"].update({"STREAM": stream})
    args = kwargs["ARGS"]

    task_name = kwargs.get("TASK_NAME")
    # debug = kwargs["DEBUG"]

    wrapper = kwargs.get("WRAPPER")

    task_kwargs = {
        "wrapper": wrapper,
        "force": kwargs["FORCE"],
        "check_existence": kwargs["CHECK_EXISTENCE"],
        "print_namelist": kwargs["PRINT_NAMELIST"]
    }

    if args != "":
        print(args)
        for arg in args.split(";"):
            print(arg)
            parts = arg.split("=")
            print(parts)
            print(len(parts))
            if len(parts) == 2:
                task_kwargs.update({parts[0]: parts[1]})

    dtg = kwargs["DTG"]
    dtgbeg = kwargs["DTGBEG"]
    progress = {
        "DTG": dtg,
        "DTGBEG": dtgbeg,
        "DTGPP": dtg
    }
    # This will also handle call to sys.exit(), i.e. Client.__exit__ will still be called.
    with scheduler.EcflowClient(server, task):

        #scheduler_pythonpath = system_variables["SCHEDULER_PYTHONPATH"]
        # Dummy commands to try out your self
        #print(f"PYTHONPATH={scheduler_pythonpath} && %EXP_DIR%/bin/ECF_status_exp %EXP_DIR%/scheduler.json " +
        #    "%ECF_NAME% %ECF_TRYNO% %ECF_PASS% -ecf_rid  -submission_id ")
        #print("PYTHONPATH={scheduler_pythonpath} && %EXP_DIR%/bin/ECF_kill_exp %EXP_DIR%/scheduler.json " +
        #    "%ECF_NAME% %ECF_TRYNO% %ECF_PASS% -ecf_rid %ECF_RID% -submission_id ")

        logging.info("Running task %s", task_name)
        classes = inspect.getmembers(experiment_tasks, inspect.isclass)
        task_class = None
        for cclass in classes:
            cname = cclass[0]
            ctask = cclass[1]
            if cname == task_name:
                task_class = ctask

        if task_class is None:
            raise Exception("Class not found for task " + task_name)

        logging.info(task_class.__name__)
        task_class(task, task_config, system, system_file_paths, progress, **task_kwargs).run()

if __name__ == "__main__":
    # Get ecflow variables
    kwargs_main = parse_ecflow_vars()

    # Set system vars for host
    LIB = kwargs_main["LIB"]
    HOST = kwargs_main["HOST"]
    system_vars_main = read_system_vars(LIB, host=HOST)

    # Server settings
    server_settings_main = read_ecflow_server_file(LIB)

    # Experiment configuration
    ENSMBR = kwargs_main["ENSMBR"]
    if ENSMBR == "":
        ENSMBR = None
    exp_configuration_main = read_exp_configuration(LIB, ensmbr=ENSMBR)
    # System file paths for host
    system_file_paths_main = read_system_file_paths(LIB, host=HOST)

    default_main(system_vars_main, server_settings_main, exp_configuration_main,
                 system_file_paths_main, **kwargs_main)

'''
%end"
'''
