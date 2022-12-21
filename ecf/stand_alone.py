"""Default ecflow container."""
# @ENV_SUB1@
import json
import os
import logging
from experiment_tasks import get_task

# @ENV_SUB2@


def read_exp_configuration(config_file, ensmbr=None):
    """Read experiment configuration.

    The task knows which host it runs on and which member it is

    """
    with open(config_file, mode="r", encoding="utf-8") as file_handler:
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
    config = "/home/sbu/sfx_home/test_default/exp_configuration2.json"


    # Set system vars for host
    kwargs = {
        "HOST": "0",
        "ENSMBR": "",
        "DTG": "202212010300",
        "DTGBEG": "202212010000",
        "DTGPP": "202212010300",
        "FAMILY1": ""
    }

    # Experiment configuration
    ENSMBR = kwargs["ENSMBR"]
    if ENSMBR == "":
        ENSMBR = None
    exp_config = read_exp_configuration(config, ensmbr=ENSMBR)

    if "TASK" not in exp_config:
        exp_config.update({
            "TASK": {
                "WRAPPER": "",
                "VAR_NAME": "",
                "ARGS": {}
            }
        })

    default_main(task_name, exp_config, loglevel)
