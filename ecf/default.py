"""Default ecflow container."""
# @ENV_SUB1@
import json
import logging
import experiment_scheduler as scheduler
from experiment import ConfigurationFromJsonFile
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


def default_main(**kwargs):
    """Ecflow container default method."""
    debug = kwargs.get("DEBUG")
    if debug is None:
        debug = False
    if debug:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                            level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    ecf_name = kwargs.get("ECF_NAME")
    ecf_pass = kwargs.get("ECF_PASS")
    ecf_tryno = kwargs.get("ECF_TRYNO")
    ecf_rid = kwargs.get("ECF_RID")
    task = scheduler.EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid)

    config = kwargs.get("CONFIG")
    config = ConfigurationFromJsonFile(config)
    config.update_setting("GENERAL#STREAM", kwargs.get("STREAM"))
    config.update_setting("GENERAL#ENSMBR", kwargs.get("ENSMBR"))
    args = kwargs.get("ARGS")

    task_name = kwargs.get("TASK_NAME")
    args_dict = {
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

    progress ={
        "DTG": kwargs["DTG"],
        "DTGPP": kwargs["DTGPP"]
    }

    config.update_setting("PROGRESS", progress)
    task_info = {
        "WRAPPER": kwargs.get("WRAPPER"),
        "VAR_NAME": kwargs.get("VAR_NAME"),
        "ARGS": args_dict,
    }
    config.update_setting("TASK", task_info)

    # This will also handle call to sys.exit(), i.e. Client.__exit__ will still be called.
    with scheduler.EcflowClient(config.server, task):

        logging.info("Running task %s", task_name)
        get_task(task.ecf_task, config).run()
        logging.info("Finished task %s", task_name)


if __name__ == "__main__":
    # Get ecflow variables
    kwargs_main = parse_ecflow_vars()

    default_main(**kwargs_main)

'''    # noqa
%end"  # noqa
'''    # noqa
