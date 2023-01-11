"""Command line interfaces for starting/checking/stating jobs."""
import sys
import logging
from argparse import ArgumentParser
import experiment_scheduler as scheduler
import experiment


def parse_submit_cmd_exp(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("ECF_submit task to ecflow")
    parser.add_argument('-config', dest="config_file", type=str, help="Configuration file")
    parser.add_argument('-task', type=str, help="Task name")
    parser.add_argument('-task_job', type=str, help="Task job file")
    parser.add_argument('-output', type=str, help="Output file")
    parser.add_argument('-template', dest="template_job", type=str, help="Template")
    parser.add_argument('-troika', type=str, help="Troika", default="troika", required=False)
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
    logging.debug("scheduler location: %s", scheduler.__file__)
    config = kwargs.get("config_file")
    config = experiment.ConfigurationFromJsonFile(config)
    troika_config = config.get_setting("TROIKA#CONFIG")
    submission_defs = scheduler.TaskSettings(config.env_submit)
    sub = scheduler.NoSchedulerSubmission(submission_defs)
    sub.submit(
        kwargs.get("task"),
        config,
        kwargs.get("template_job"),
        kwargs.get("task_job"),
        kwargs.get("output"),
        kwargs.get("troika"),
        troika_config
    )


def run_submit_cmd_exp():
    """Run submit."""
    kwargs = parse_submit_cmd_exp(sys.argv[1:])
    submit_cmd_exp(**kwargs)
