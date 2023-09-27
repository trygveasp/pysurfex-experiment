"""Default ecflow container."""
# @ENV_SUB1@

import json
import os
from experiment import PACKAGE_NAME
from experiment.config_parser import MAIN_CONFIG_JSON_SCHEMA, ParsedConfig
from experiment.datetime_utils import ecflow2datetime_string
from experiment.logs import GLOBAL_LOGLEVEL, LoggerHandlers, logger
from experiment.scheduler.scheduler import (
    EcflowClient,
    EcflowServerFromConfig,
    EcflowTask,
)
from experiment.tasks.discover_tasks import get_task

# @ENV_SUB2@


logger.enable(PACKAGE_NAME)


def parse_ecflow_vars():
    """Parse the ecflow variables."""
    return {
        "CONFIG": "%CONFIG%",
        "WRAPPER": "%WRAPPER%",
        "ENSMBR": "%ENSMBR%",
        "DTG": "%DTG%",
        "DTGPP": "%DTGPP%",
        "STREAM": "%STREAM%",
        "TASK_NAME": "%TASK%",
        "VAR_NAME": "%VAR_NAME%",
        "LOGLEVEL": "%LOGLEVEL%",
        "ARGS": "%ARGS%",
        "ECF_NAME": "%ECF_NAME%",
        "ECF_PASS": "%ECF_PASS%",
        "ECF_TRYNO": "%ECF_TRYNO%",
        "ECF_RID": "%ECF_RID%",
    }


"""
%nopp"
"""


if __name__ == "__main__":
    # Get ecflow variables
    kwargs = parse_ecflow_vars()
    fname = str(os.getpid()) + ".json"
    with open(fname, mode="w", encoding="utf-8") as fhandler:
        json.dump(fhandler, kwargs)
    os.system(f"PySurfexScheduler {fname}")

"""    # noqa
%end"  # noqa
"""  # noqa
