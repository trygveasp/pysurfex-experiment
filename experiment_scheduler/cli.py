"""Command line interfaces for starting/checking/stating jobs."""
import os
import sys
import logging
import shutil
from argparse import ArgumentParser
import experiment_scheduler as scheduler
import experiment


def parse_submit_cmd_exp(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("ECF_submit task to ecflow")
    parser.add_argument('-config', dest="config_file", type=str, help="Configuration file")
    parser.add_argument('-task', type=str, help="Task name")
    parser.add_argument('-task_job', type=str, help="Task job file", required=False, default=None)
    parser.add_argument('-output', type=str, help="Output file", required=False, default=None)
    parser.add_argument('-template', dest="template_job", type=str, help="Template", required=False, default=None)
    parser.add_argument('-troika', type=str, help="Troika", required=False, default=None)
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
    config_file = kwargs.get("config_file")
    cwd = os.getcwd()
    if config_file is None:
       config_file = f"{cwd}/exp_configuration.json"
       logging.info("Using config file=%s", config_file)
       if os.path.exists("exp_configuration.json"):
          logging.info("Using config file=%s", config_file)
       else:
          raise FileNotFoundError("Could not find config file " + config_file)
    config = experiment.ConfigurationFromJsonFile(config_file)
    task = kwargs.get("task")
    troika = kwargs.get("troika")
    if troika is None:
        try:
            troika = config.system.get_var("TROIKA", "0")
        except Exception:
            troika = shutil.which("troika")
    troika_config = config.get_setting("TROIKA#CONFIG")
    template_job = kwargs.get("template_job")
    if template_job is None:
        scripts = config.get_setting("GENERAL#PYSURFEX_EXPERIMENT")
        if scripts is None:
            raise Exception("Could not find GENERAL#PYSURFEX_EXPERIMENT")
        else:
            template_job = f"{scripts}/ecf/stand_alone.py"
    task_job = kwargs.get("task_job")
    if task_job is None:
        task_job = f"{cwd}/{task}.job"
    output = kwargs.get("output")
    if output is None:
        output = f"{cwd}/{task}.log"
    logging.debug("Task: %s", task)
    logging.debug("config: %s", config_file)
    logging.debug("troika: %s", troika)
    logging.debug("troika_config: %s", troika_config)
    logging.debug("template_job: %s", template_job)
    logging.debug("task_job: %s", task_job)
    logging.debug("output: %s", output)
    submission_defs = scheduler.TaskSettings(config.env_submit)
    sub = scheduler.NoSchedulerSubmission(submission_defs)
    sub.submit(
        kwargs.get("task"),
        config,
        template_job,
        task_job,
        output,
        troika,
        troika_config
    )


def run_submit_cmd_exp():
    """Run submit."""
    kwargs = parse_submit_cmd_exp(sys.argv[1:])
    submit_cmd_exp(**kwargs)
