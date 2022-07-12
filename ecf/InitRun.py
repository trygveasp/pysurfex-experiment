"""InitRun ecflow container."""
import json
import scheduler
import experiment_setup


def parse_ecflow_vars_init_run():
    """Parse the ecflow variables."""
    return {
        "LIB": "%EXP_DIR%",
        "SERVER_LOGFILE": "%SERVER_LOGFILE%",
        "STREAM": "%STREAM%",
        "ECF_NAME": "%ECF_NAME%",
        "ECF_PASS": "%ECF_PASS%",
        "ECF_TRYNO": "%ECF_TRYNO%",
        "ECF_RID": "%ECF_RID%",
        "SUBMISSION_ID": "%SUBMISSION_ID%"
    }

def read_system_vars(lib):
    """Read system dict from json file."""
    with open(lib + "/exp_system_vars.json", mode="r", encoding="UTF-8") as file_handler:
        return json.load(file_handler)

def read_ecflow_server_file(lib):
    """Read ecflow host settings."""
    with open(lib + "/Env_server", mode="r", encoding="UTF-8") as file_handler:
        return json.load(file_handler)

def read_paths_to_sync(lib):
    """Read ecflow host settings."""
    with open(lib + "/paths_to_sync.json", mode="r", encoding="UTF-8") as file_handler:
        return json.load(file_handler)

def init_run_main(system_vars, server_settings, paths_to_sync, **kwargs):
    """Run main method for InitRun."""
    ecf_host = server_settings["ECF_HOST"]
    ecf_port = server_settings["ECF_PORT"]
    server_logfile = kwargs["SERVER_LOGFILE"]
    server = scheduler.EcflowServer(ecf_host, ecf_port, server_logfile)

    ecf_name = kwargs["ECF_NAME"]
    ecf_pass = kwargs["ECF_PASS"]
    ecf_tryno = kwargs["ECF_TRYNO"]
    ecf_rid = kwargs["ECF_RID"]
    submission_id = kwargs["SUBMISSION_ID"]
    task = scheduler.EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid, submission_id)

    # This will also handle call to sys.exit(), i.e. Client.__exit__ will still be called.
    with scheduler.EcflowClient(server, task):
        print("Scheduler path: ", scheduler.__file__)
        # InitRun always runs from HOST0

        stream = kwargs["STREAM"]
        if stream == "":
            stream = None
        experiment_setup.init_run(system_vars, paths_to_sync, stream_nr=stream)

if __name__ == "__main__":
    kwargs_main = parse_ecflow_vars_init_run()

    LIB = kwargs_main["LIB"]
    system_vars_main = read_system_vars(LIB)
    server_settings_main = read_ecflow_server_file(LIB)
    paths_to_sync_main = read_paths_to_sync(LIB)
    init_run_main(system_vars_main, server_settings_main, paths_to_sync_main, **kwargs_main)
