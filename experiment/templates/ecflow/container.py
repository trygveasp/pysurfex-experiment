"""Default ecflow container."""
# @ENV_SUB1@

import json
import os

# @ENV_SUB2@

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
        json.dump(kwargs, fhandler)
    print(f"singularity exec --bind /lustre:/lustre /home/trygveasp/projects/pysurfex-experiment/trygveasp/feature/task_in_container/pysurfex-experiment.sif PySurfexScheduler {fname}")
    os.system(f"singularity exec --bind /lustre:/lustre /home/trygveasp/projects/pysurfex-experiment/trygveasp/feature/task_in_container/pysurfex-experiment.sif PySurfexScheduler {fname}")

"""    # noqa
%end"  # noqa
"""  # noqa
