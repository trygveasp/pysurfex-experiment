"""Command line interfaces for starting/checking/stating jobs."""
import sys
import logging
from argparse import ArgumentParser
import scheduler


def parse_submit_cmd_exp(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("ECF_submit task to ecflow")
    parser.add_argument('-sub', dest="submission_file",  type=str, help="JSON file with experiment settings")
    parser.add_argument('-config', dest="config_file", type=str, help="Configuration file")
    parser.add_argument('-task', type=str, help="Task name")
    parser.add_argument('-task_job', type=str, help="Task job file")
    parser.add_argument('-output', type=str, help="Output file")
    parser.add_argument('-template', dest="template_job", type=str, help="Template")
    parser.add_argument('-troika', type=str, help="Troika", default="troika", required=False)
    parser.add_argument('-troika_config', type=str, help="Troika config", required=False, default="config.yml")
    parser.add_argument('--debug', dest="debug", action="store_true", help="Debug information")
    # parser.add_argument('--version', action='version', version=__version__)

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

    logging.debug("kwargs %s", str(kwargs))
    print(scheduler.__file__)
    submission_defs = scheduler.TaskSettingsJson(kwargs.get('submission_file'))
    sub = scheduler.NoSchedulerSubmission(submission_defs)
    sub.submit(
        kwargs.get("task"),
        kwargs.get("config_file"),
        kwargs.get("template_job"),
        kwargs.get("task_job"),
        kwargs.get("output"),
        kwargs.get("troika"),
        kwargs.get("troika_config")
    )


def run_submit_cmd_exp():
    """Run submit."""
    kwargs = parse_submit_cmd_exp(sys.argv[1:])
    submit_cmd_exp(**kwargs)
