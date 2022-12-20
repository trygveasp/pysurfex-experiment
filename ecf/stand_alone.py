"""Default ecflow container."""
# @ENV_SUB1@
import json
import os
import logging
from experiment_tasks import get_task
# @ENV_SUB2@

def read_system_vars(lib, host="0"):
    """Read system dict from json file."""
    with open(lib + "/exp_system_vars.json", mode="r", encoding="utf-8") as file_handler:
        return json.load(file_handler)[host]


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


def default_main(task, config, loglevel):
    """Execute default main.

    Args:
        task (str): Task name
        config (str.): Config file
        loglevel (str): Loglevel
    """
    # logger = get_logger(__name__, loglevel)
    logging.basicConfig(level=loglevel)
    logging.info("Running task %s", task)

    get_task(task, config).run()
    logging.info("Finished task %s", task)
    print("Finished")


if __name__ == "__main__":
    task_name = "@STAND_ALONE_TASK_NAME@"
    # loglevel = "@STAND_ALONE_TASK_LOGLEVEL@"
    loglevel = logging.DEBUG
    config = "@STAND_ALONE_TASK_CONFIG@"

    # Set system vars for host
    kwargs = {
        "LIB": os.getcwd(),
        "HOST": "0",
        "ENSMBR": "",
        "DTG": "202212010300",
        "DTGBEG": "202212010000",
        "DTGPP": "202212010300",
        "FAMILY1": ""
    }
    LIB = kwargs["LIB"]
    HOST = kwargs["HOST"]
    system_vars = read_system_vars(LIB, host=HOST)

    # Experiment configuration
    ENSMBR = kwargs["ENSMBR"]
    if ENSMBR == "":
        ENSMBR = None
    exp_config = read_exp_configuration(LIB, ensmbr=ENSMBR)
    # System file paths for host
    system_file_paths = read_system_file_paths(LIB, host=HOST)

    dtg = kwargs["DTG"]
    dtgbeg = kwargs["DTGBEG"]
    progress = {
        "DTG": dtg,
        "DTGBEG": dtgbeg,
        "DTGPP": dtg
    }
    exp_config.update({"SYSTEM_FILE_PATHS": system_file_paths})
    exp_config.update({"SYSTEM_VARS": system_vars})
    exp_config.update({"PROGRESS": progress})
    if "TASK" not in exp_config:
        exp_config.update({"TASK":
            {
                "WRAPPER": "",
                "VAR_NAME": ""
                }
            })

    default_main(task_name, exp_config, loglevel)
