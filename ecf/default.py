"""Default ecflow container."""
# @ENV_SUB1@
import json
import logging
import scheduler
from experiment_tasks import get_task
# @ENV_SUB2@

def parse_ecflow_vars():
    """Parse the ecflow variables."""
    return {
        "CONFIG": "%CONFIG%",
        "WRAPPER": "%WRAPPER%",
        "ENSMBR": "%ENSMBR%",
        "DTG": "%DTG%",
        "DTGPP": "%DTGPP%",
        "stream": "%STREAM%",
        "TASK_NAME": "%TASK%",
        "VAR_NAME": "%VAR_NAME%",
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
        "ECF_RID": "%ECF_RID%"
    }


'''
%nopp"
'''


def read_exp_configuration(config_file, ensmbr=None):
    """Read experiment configuration.

    The task knows which host it runs on and which member it is

    """
    with open(config_file, mode="r", encoding="utf-8") as file_handler:
        if ensmbr is None:
            return json.load(file_handler)
        return json.load(file_handler)[ensmbr]


def default_main(config, **kwargs):
    """Ecflow container default method."""
    debug = kwargs.get("DEBUG")
    if debug is None:
        debug = False
    if debug:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                            level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    ecf_host = config["SCHEDULER"].get("ECF_HOST")
    ecf_port = config["SCHEDULER"].get("ECF_PORT")
    server = scheduler.EcflowServer(ecf_host, ecf_port)

    ecf_name = kwargs.get("ECF_NAME")
    ecf_pass = kwargs.get("ECF_PASS")
    ecf_tryno = kwargs.get("ECF_TRYNO")
    ecf_rid = kwargs.get("ECF_RID")
    task = scheduler.EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid)

    stream = kwargs.get("STREAM")
    config["GENERAL"].update({"STREAM": stream})
    args = kwargs.get("ARGS")

    task_name = kwargs.get("TASK_NAME")
    wrapper = kwargs.get("WRAPPER")

    args_dict = {
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
                args_dict.update({parts[0]: parts[1]})

    dtg = kwargs["DTG"]
    dtgpp = kwargs["DTGPP"]
    config["PROGRESS"].update({
        "DTG": dtg,
        "DTGPP": dtgpp
        })
    if "TASK" not in config:
        config.update({"TASK":
            {
                "WRAPPER": wrapper,
                "VAR_NAME": kwargs.get("VAR_NAME"),
                "ARGS": args_dict,
                }
            })

    # This will also handle call to sys.exit(), i.e. Client.__exit__ will still be called.
    with scheduler.EcflowClient(server, task):

        logging.info("Running task %s", task_name)
        get_task(task.ecf_task, config).run()
        logging.info("Finished task %s", task_name)


if __name__ == "__main__":
    # Get ecflow variables
    kwargs_main = parse_ecflow_vars()

    # Experiment configuration
    ENSMBR = kwargs_main["ENSMBR"]
    if ENSMBR == "":
        ENSMBR = None

    exp_configuration_main = read_exp_configuration(kwargs_main["CONFIG"], ensmbr=ENSMBR)
    default_main(exp_configuration_main, **kwargs_main)

'''    # noqa
%end"  # noqa
'''    # noqa
