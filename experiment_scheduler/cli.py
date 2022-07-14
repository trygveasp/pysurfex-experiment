"""Command line interfaces for starting/checking/stating jobs."""
import sys
import logging
import json
from argparse import ArgumentParser
import scheduler

__version__ = "0.0.1"


def parse_submit_cmd_exp(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("ECF_submit task to ecflow")
    parser.add_argument('-exp', type=str, help="JSON file with experiment settings")
    parser.add_argument('-dtg', dest="dtg", type=str, help="DTG", default=None, required=False)
    # parser.add_argument('-dtgbeg', dest="dtgbeg", type=str, help="DTGBEG", default=None,
    # required=False)
    parser.add_argument('-ecf_name', type=str, help="Name of ECF Task")
    parser.add_argument('-ecf_tryno', type=str, help="ECF try number")
    parser.add_argument('-ecf_pass', type=str, help="Name of ECF password")

    parser.add_argument('-stream', type=str, nargs="?", help="Stream", required=False, default=None)
    parser.add_argument('-ecf_rid', nargs='?', type=str, default=None, required=False,
                        help="ECF remote id")
    parser.add_argument('-ensmbr', dest="ensmbr", nargs="?", type=int, help="Ensemble member",
                        required=False, default=None)
    parser.add_argument('--db', dest="dbfile", type=str, nargs="?", help="Database", required=False,
                        default=None)
    parser.add_argument('--debug', dest="debug", action="store_true", help="Debug information")
    parser.add_argument('--version', action='version', version=__version__)

    if len(argv) == 0:
        parser.print_help()
        sys.exit()

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def submit_cmd_exp(**kwargs):
    """Submit task."""
    debug = kwargs.get("debug")
    if debug is None:
        debug = False
    if debug:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                            level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    logging.info("************ ECF_submit_exp ******************")

    exp_settings_file = kwargs.get("exp")
    with open(exp_settings_file, mode="r", encoding="utf-8") as file_handler:
        exp_settings = json.load(file_handler)
    if isinstance(exp_settings["submit_exceptions"], str):
        submit_exceptions = json.load(open(exp_settings["submit_exceptions"], mode="r",
                                      encoding="UTF-8"))
        exp_settings["submit_exceptions"] = submit_exceptions
    kwargs.update({"submit_exceptions": exp_settings["submit_exceptions"]})
    kwargs.update({"env_file": exp_settings["env_file"]})
    if isinstance(exp_settings["env_submit"], str):
        env_submit = json.load(open(exp_settings["env_submit"], mode="r", encoding="UTF-8"))
        exp_settings["env_submit"] = env_submit
    kwargs.update({"env_submit": exp_settings["env_submit"]})
    if isinstance(exp_settings["env_server"], str):
        env_server = json.load(open(exp_settings["env_server"], mode="r", encoding="UTF-8"))
        exp_settings["env_server"] = env_server
    kwargs.update({"env_server": exp_settings["env_server"]})
    kwargs.update({"joboutdir": exp_settings["joboutdir"]})
    kwargs.update({"logfile": exp_settings["logfile"]})

    logging.debug("kwargs %s", str(kwargs))
    scheduler.submit_cmd(**kwargs)


def parse_status_cmd_exp(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Status of EcFlow task")
    parser.add_argument('-exp', type=str, help="JSON file with experiment settings")
    parser.add_argument('-ecf_name', type=str, help="ECF_NAME", required=True)
    parser.add_argument('-ecf_tryno', type=str, help="ECF_TRYNO", required=True)
    parser.add_argument('-ecf_pass', type=str, help="ECF_PASS", required=True)
    parser.add_argument('-ecf_rid', type=str, help="ECF_RID", required=False, nargs="?",
                        default=None)
    parser.add_argument('-submission_id', type=str, help="SUBMISSION_ID")
    parser.add_argument('--debug', dest="debug", action="store_true", help="Debug information")
    parser.add_argument('--version', action='version', version=__version__)

    if len(argv) == 0:
        parser.print_help()
        sys.exit()

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def status_cmd_exp(**kwargs):
    """Set status."""
    debug = kwargs.get("debug")
    if debug is None:
        debug = False
    if debug:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                            level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    logging.info("************ ECF_status_exp ******************")

    exp_settings_file = kwargs.get("exp")
    with open(exp_settings_file, mode="r", encoding="utf-8") as file_handler:
        exp_settings = json.load(file_handler)
    if isinstance(exp_settings["submit_exceptions"], str):
        submit_exceptions = json.load(open(exp_settings["submit_exceptions"], mode="r",
                                           encoding="UTF-8"))
        exp_settings["submit_exceptions"] = submit_exceptions
    kwargs.update({"submit_exceptions": exp_settings["submit_exceptions"]})
    kwargs.update({"env_file": exp_settings["env_file"]})
    if isinstance(exp_settings["env_submit"], str):
        env_submit = json.load(open(exp_settings["env_submit"], mode="r", encoding="UTF-8"))
        exp_settings["env_submit"] = env_submit
    kwargs.update({"env_submit": exp_settings["env_submit"]})
    if isinstance(exp_settings["env_server"], str):
        env_server = json.load(open(exp_settings["env_server"], mode="r", encoding="UTF-8"))
        exp_settings["env_server"] = env_server
    kwargs.update({"env_server": exp_settings["env_server"]})
    kwargs.update({"joboutdir": exp_settings["joboutdir"]})

    logging.debug("kwargs %s", str(kwargs))
    scheduler.status_cmd(**kwargs)


def parse_kill_cmd_exp(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Kill EcFlow task and handle abort")
    parser.add_argument('-exp', type=str, help="JSON file with experiment settings")
    parser.add_argument("-ecf_name", dest='ecf_name', type=str, help="ECF_NAME", required=True)
    parser.add_argument("-ecf_tryno", dest='ecf_tryno', type=str, help="ECF_TRYNO", required=True)
    parser.add_argument("-ecf_pass", dest='ecf_pass', type=str, help="ECF_PASS", required=True)
    parser.add_argument('-ecf_rid', dest='ecf_rid', type=str, help="ECF_RID", required=False,
                        nargs="?", default=None)
    parser.add_argument('-submission_id', type=str, help="SUBMISSION_ID")
    parser.add_argument('--debug', dest="debug", action="store_true", help="Debug information")
    parser.add_argument('--version', action='version', version=__version__)

    if len(argv) == 0:
        parser.print_help()
        sys.exit()

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def kill_cmd_exp(**kwargs):
    """Kill the task."""
    debug = kwargs.get("debug")
    if debug is None:
        debug = False
    if debug:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                            level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    logging.info("************ ECF_kill_exp ******************")

    exp_settings = json.load(open(kwargs["exp"], mode="r", encoding="UTF-8"))
    if isinstance(exp_settings["submit_exceptions"], str):
        submit_exceptions = json.load(open(exp_settings["submit_exceptions"], mode="r",
                                      encoding="UTF-8"))
        exp_settings["submit_exceptions"] = submit_exceptions
    kwargs.update({"submit_exceptions": exp_settings["submit_exceptions"]})
    kwargs.update({"env_file": exp_settings["env_file"]})
    if isinstance(exp_settings["env_submit"], str):
        env_submit = json.load(open(exp_settings["env_submit"], mode="r", encoding="UTF-8"))
        exp_settings["env_submit"] = env_submit
    kwargs.update({"env_submit": exp_settings["env_submit"]})
    if isinstance(exp_settings["env_server"], str):
        env_server = json.load(open(exp_settings["env_server"], mode="r", encoding="UTF-8"))
        exp_settings["env_server"] = env_server
    kwargs.update({"env_server": exp_settings["env_server"]})
    kwargs.update({"joboutdir": exp_settings["joboutdir"]})
    kwargs.update({"logfile": exp_settings["logfile"]})

    logging.debug("kwargs %s", str(kwargs))
    scheduler.kill_cmd(**kwargs)
