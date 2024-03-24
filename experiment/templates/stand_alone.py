"""Default ecflow container."""
# @ENV_SUB1@


from experiment import PACKAGE_NAME
from experiment.config_parser import MAIN_CONFIG_JSON_SCHEMA, ParsedConfig
from experiment.logs import logger
from experiment.tasks.discover_tasks import get_task

# @ENV_SUB2@


logger.enable(PACKAGE_NAME)


def stand_alone_main(task, config_file):
    """Execute default main.

    Args:
    ----
        task (str): Task name
        config_file (str): Config file

    """
    config = ParsedConfig.from_file(config_file, json_schema=MAIN_CONFIG_JSON_SCHEMA)

    logger.info("Running task {}", task)
    get_task(task, config).run()
    logger.info("Finished task {}", task)


if __name__ == "__main__":
    TASK_NAME = "@STAND_ALONE_TASK_NAME@"
    CONFIG = "@STAND_ALONE_TASK_CONFIG@"

    stand_alone_main(TASK_NAME, CONFIG)
