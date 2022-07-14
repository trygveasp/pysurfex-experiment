"""Log progress ecflow container."""
import json
import scheduler


def parse_ecflow_vars_logprogress():
    """Parse the ecflow variables."""
    return {
        "LIB": "%EXP_DIR%",
        "SERVER_LOGFILE": "%SERVER_LOGFILE%",
        "STREAM": "%STREAM%",
        "NEXT_DTG": "%DTG_NEXT%",
        "DTGBEG": "%DTGBEG%",
        "ECF_NAME": "%ECF_NAME%",
        "ECF_PASS": "%ECF_PASS%",
        "ECF_TRYNO": "%ECF_TRYNO%",
        "ECF_RID": "%ECF_RID%",
        "SUBMISSION_ID": "%SUBMISSION_ID%"
    }


def read_ecflow_server_file_logprogress(lib):
    """Read ecflow host settings."""
    with open(lib + "/Env_server", mode="r", encoding="UTF-8") as file_handler:
        return json.load(file_handler)


def log_progress_main(server_settings, **kwargs):
    """Log progress to file."""
    server_logfile = kwargs["SERVER_LOGFILE"]
    ecf_host = server_settings["ECF_HOST"]
    ecf_port = server_settings["ECF_PORT"]
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

        dtgbeg = kwargs["DTGBEG"]
        next_dtg = kwargs["NEXT_DTG"]
        stream = kwargs["STREAM"]
        work_dir = kwargs["LIB"]
        stream_text = ""
        if stream is not None and stream != "":
            stream_text = "_stream_" + stream
        progress_file = work_dir + "/progress" + stream_text + ".json"

        with open(progress_file, mode="r", encoding="utf-8") as file_handler:
            progress = json.load(file_handler)
        # Update progress
        progress.update({
            "DTG": next_dtg,
            "DTGBEG": dtgbeg
        })
        with open(progress_file, mode="w", encoding="utf-8") as file_handler:
            json.dump(progress, file_handler, indent=2)


if __name__ == "__main__":
    kwargs_main = parse_ecflow_vars_logprogress()
    LIB = kwargs_main["LIB"]
    server_settings_main = read_ecflow_server_file_logprogress(LIB)
    log_progress_main(server_settings_main, **kwargs_main)
