"""Default ecflow container."""
# @ENV_SUB1@
import logging
from experiment import ConfigurationFromJsonFile
from experiment_tasks import get_task

# @ENV_SUB2@


def stand_alone_main(task, config_file, loglevel):
    """Execute default main.

    Args:
        task (str): Task name
        config (str.): Config file
        loglevel (str): Loglevel
    """

    # logger = get_logger(__name__, loglevel)
    logging.basicConfig(level=loglevel)
    logging.info("Running task %s", task)

    config = ConfigurationFromJsonFile(config_file)

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
    ensmbr = kwargs["ENSMBR"]
    if ensmbr == "":
        ensmbr = None

    config.update_setting("GENERAL#ENSMBR", ensmbr)
    task_info= {
        "WRAPPER": "",
        "VAR_NAME": "",
        "ARGS": {}
    }
    config.update_setting("TASK", task_info)
    get_task(task, config).run()
    logging.info("Finished task %s", task)
    print("Finished")


if __name__ == "__main__":
    TASK_NAME = "@STAND_ALONE_TASK_NAME@"
    # loglevel = "@STAND_ALONE_TASK_LOGLEVEL@"
    LOGLEVEL = logging.DEBUG
    CONFIG = "@STAND_ALONE_TASK_CONFIG@"

    stand_alone_main(TASK_NAME, CONFIG, LOGLEVEL)

