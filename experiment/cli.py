"""Client interfaces for offline experiment scripts."""
import json
import os
import sys
from argparse import ArgumentParser

from . import PACKAGE_NAME, __version__
from .config_parser import ParsedConfig
from .experiment import ExpFromConfig, ExpFromFilesDepFile
from .logs import logger
from .scheduler.scheduler import EcflowServerFromConfig
from .scheduler.submission import NoSchedulerSubmission, TaskSettings
from .suites import get_defs
from .toolbox import Platform


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


def parse_surfex_script(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Surfex offline run script")
    parser.add_argument(
        "action",
        type=str,
        help="Action",
        choices=["start", "prod", "continue", "testbed", "install", "climate", "co"],
    )
    parser.add_argument(
        "-config", dest="config", help="Config file", type=str, default=None
    )
    parser.add_argument(
        "-exp_name", dest="exp", help="Experiment name", type=str, default=None
    )
    parser.add_argument(
        "--wd", help="Experiment working directory", type=str, default=None
    )

    parser.add_argument(
        "-dtg", help="DateTimeGroup (YYYYMMDDHH)", type=str, required=False, default=None
    )
    parser.add_argument(
        "-dtgend",
        help="DateTimeGroup (YYYYMMDDHH)",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "--suite",
        type=str,
        default="surfex",
        required=False,
        help="Type of suite definition",
    )
    parser.add_argument("--stream", type=str, default=None, required=False, help="Stream")

    # co
    parser.add_argument(
        "--file", type=str, default=None, required=False, help="File to checkout"
    )
    parser.add_argument("--version", action="version", version=__version__)

    if len(argv) == 0:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def surfex_script(**kwargs):
    """Modify or start an experiment suite.

    Args:
        kwargs (dict): Input arguments.

    Raises:
        NotImplementedError: Options missing
        RuntimeError: Could not start experiment

    """
    logger.enable(PACKAGE_NAME)
    logger.info("************ PySurfexExp ******************")

    action = kwargs["action"]
    exp = kwargs.get("exp")

    stream = kwargs.get("stream")

    # Others
    dtg = kwargs["dtg"]
    dtgend = kwargs["dtgend"]
    suite = kwargs["suite"]

    begin = kwargs.get("begin")
    if begin is None:
        begin = True

    config_file = kwargs.get("config")

    if config_file is None:
        work_dir = kwargs.get("wd")
        if work_dir is None:
            work_dir = f"{os.getcwd()}"
            logger.info("Setting working directory from current directory: {}", work_dir)

        # Find experiment
        if exp is None:
            logger.info("Setting EXP from WD: {}", work_dir)
            exp = work_dir.split("/")[-1]
            logger.info("EXP = {}", exp)

        # Set experiment from files. Should be existing now after setup
        exp_dependencies_file = f"{work_dir}/exp_dependencies.json"
        sfx_exp = ExpFromFilesDepFile(exp_dependencies_file, stream=stream)
        config_file = f"{work_dir}/exp_configuration.json"
        sfx_exp.dump_json(config_file, indent=2)

    config = ParsedConfig.from_file(config_file)
    work_dir = config.get_value("system.exp_dir")

    if "action" == "mon":
        # TODO

        raise NotImplementedError
    else:
        # Some kind of start
        if action == "start" and dtg is None:
            raise RuntimeError("You must provide -dtg to start a simulation")
        elif action == "climate":
            if suite is not None and suite != "climate":
                raise RuntimeError(
                    "Action was climate but you also specified a suite not being "
                    + f"climate: {suite}"
                )
            suite = "climate"
        elif action == "testbed":
            if suite is not None and suite != "testbed":
                raise RuntimeError(
                    "Action was climate but you also specified a suite not being "
                    + +f"testbed: {suite}"
                )
            suite = "testbed"
        elif action == "install":
            raise NotImplementedError

        progress = {}
        if action.lower() == "prod" or action.lower() == "continue":
            if dtgend is not None:
                progress.update({"end": dtgend})
            if dtg is not None:
                progress.update({"basetime": dtg})
        else:
            if action == "start":
                if dtg is None:
                    raise RuntimeError("No DTG was provided!")

                progress.update({"start": dtg})
                progress.update({"basetime": dtg})
                progress.update({"validtime": dtg})
                progress.update({"basetime_pp": dtg})
                if dtgend is not None:
                    progress.update({"end": dtgend})

        if config_file is None:
            # Set experiment from files. Should be existing now after setup
            exp_dependencies_file = f"{work_dir}/exp_dependencies.json"
            sfx_exp = ExpFromFilesDepFile(
                exp_dependencies_file, stream=stream, progress=progress
            )
            config_file = f"{work_dir}/exp_configuration.json"
        else:
            with open(config_file, mode="r", encoding="utf-8") as fhandler:
                config = json.load(fhandler)
            sfx_exp = ExpFromConfig(config, progress)
        sfx_exp.dump_json(config_file, indent=2)
        config = ParsedConfig.from_file(config_file)

        # Create and start the suite
        case = config.get_value("general.case")
        sfx_data = Platform(config).get_system_value("sfx_exp_data")
        os.makedirs(sfx_data, exist_ok=True)
        def_file = f"{sfx_data}/{case}_{suite}.def"

        logger.info("Creating def file: {}", def_file)
        defs = get_defs(config, suite)
        defs.save_as_defs(def_file)
        server = EcflowServerFromConfig(config)
        server.start_suite(defs.suite_name, def_file, begin=begin)


def parse_update_config(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Update Surfex offline configuration")
    parser.add_argument(
        "-exp_name", dest="exp", help="Experiment name", type=str, default=None
    )
    parser.add_argument(
        "--wd", help="Experiment working directory", type=str, default=None
    )
    parser.add_argument("--version", action="version", version=__version__)

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def update_config(**kwargs):
    """Update the experiment json file configurations."""
    logger.enable(PACKAGE_NAME)
    exp = kwargs.get("exp")
    work_dir = kwargs.get("wd")

    # Find experiment
    if work_dir is None:
        work_dir = os.getcwd()
        logger.info("Setting current working directory as WD: {}", work_dir)
    if exp is None:
        logger.info("Setting EXP from WD: {}", work_dir)
        exp = work_dir.split("/")[-1]
        logger.info("EXP = {}", exp)

    # Set experiment from files. Should be existing now after setup
    exp_dependencies_file = f"{work_dir}/exp_dependencies.json"
    sfx_exp = ExpFromFilesDepFile(exp_dependencies_file)
    sfx_exp.dump_json(f"{work_dir}/exp_configuration.json", indent=2)

    logger.info("Configuration was updated!")


def surfex_exp(argv=None):
    """Surfex exp script entry point."""
    if argv is None:
        argv = sys.argv[1:]
    kwargs = parse_surfex_script(argv)
    surfex_script(**kwargs)


def surfex_exp_config(argv=None):
    """Surfex exp config entry point."""
    if argv is None:
        argv = sys.argv[1:]
    kwargs = parse_update_config(argv)
    update_config(**kwargs)


def parse_submit_cmd_exp(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Submit task")
    parser.add_argument(
        "-config", dest="config_file", type=str, help="Configuration file"
    )
    parser.add_argument("-task", type=str, help="Task name")
    parser.add_argument(
        "-task_job", type=str, help="Task job file", required=False, default="Task.job"
    )
    parser.add_argument(
        "-output",
        type=str,
        help="Output file",
        required=False,
        default=f"{os.getcwd()}/Task.log",
    )
    parser.add_argument(
        "-template",
        dest="template_job",
        type=str,
        help="Template",
        required=False,
        default=None,
    )
    parser.add_argument("-troika", type=str, help="Troika", required=False, default=None)
    parser.add_argument(
        "--background", dest="background", action="store_true", help="Run in background"
    )
    parser.add_argument("--version", action="version", version=__version__)

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
    logger.enable(PACKAGE_NAME)
    logger.info("************ ECF_submit_exp ******************")
    config_file = kwargs.get("config_file")
    cwd = os.getcwd()
    if config_file is None:
        config_file = f"{cwd}/exp_configuration.json"
        logger.info("Using config file={}", config_file)
        if os.path.exists("exp_configuration.json"):
            logger.info("Using config file={}", config_file)
        else:
            raise FileNotFoundError("Could not find config file " + config_file)
    config = ParsedConfig.from_file(config_file)
    task = kwargs.get("task")

    template_job = kwargs.get("template_job")
    if template_job is None:
        try:
            scripts = config.get_value("system.pysurfex_experiment")
        except AttributeError as exc:
            raise AttributeError("Could not find system.pysurfex_experiment") from exc
        else:
            template_job = f"{scripts}/experiment/templates/stand_alone.py"
    task_job = kwargs.get("task_job")
    if task_job is None:
        task_job = f"{cwd}/{task}.job"
    output = kwargs.get("output")
    if output is None:
        output = f"{cwd}/{task}.log"
    logger.debug("Task: {}", task)
    logger.debug("config: {}", config_file)
    logger.debug("template_job: {}", template_job)
    logger.debug("task_job: {}", task_job)
    logger.debug("output: {}", output)
    if kwargs.get("background"):
        update = {"submission": {"default_submit_type": "background"}}
        config = config.copy(update=update)
    submission_defs = TaskSettings(config)
    sub = NoSchedulerSubmission(submission_defs)
    sub.submit(kwargs.get("task"), config, template_job, task_job, output)


def run_submit_cmd_exp(argv=None):
    """Run submit."""
    if argv is None:
        argv = sys.argv[1:]
    kwargs = parse_submit_cmd_exp(argv)
    submit_cmd_exp(**kwargs)


def parse_run_task(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Run a task")
    parser.add_argument(
        "infile",
        type=str,
        help="Input file"
    )
    parser.add_argument("--version", action="version", version=__version__)

    if len(argv) == 0:
        parser.print_help()
        sys.exit()

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def run_task_in_container(argv=None):
    """Run a task.

    Args:
        argv (list, optional): Arguments. Defaults to None.
    """
    if argv is None:
        argv = sys.argv[1:]
    kwargs = parse_run_task(argv=argv)

    with open(kwargs["infile"], mode="r", encoding="utf-8") as fhandler:
        kwargs = json.load(fhandler)

    config = kwargs.get("CONFIG")
    config = ParsedConfig.from_file(config, json_schema=MAIN_CONFIG_JSON_SCHEMA)

    # Reset loglevel according to (in order of priority):
    #     (a) Configs in ECFLOW UI
    #     (b) What was originally set in the config file
    #     (c) The default `GLOBAL_LOGLEVEL` if none of the above is found.
    loglevel = kwargs.get(
        "LOGLEVEL", config.get_value("general.loglevel", GLOBAL_LOGLEVEL)
    ).upper()
    logger.configure(handlers=LoggerHandlers(default_level=loglevel))
    logger.info("Loglevel={}", loglevel)

    ecf_name = kwargs.get("ECF_NAME")
    ecf_pass = kwargs.get("ECF_PASS")
    ecf_tryno = kwargs.get("ECF_TRYNO")
    ecf_rid = kwargs.get("ECF_RID")
    if ecf_name is None:
        task_name = kwargs.get("TASK_NAME")
        logger.info("Running task {}", task_name)
        args = kwargs.get("ARGS")
        args_dict = {}
        if args != "":
            logger.debug("args={}", args)
            for arg in args.split(";"):
                parts = arg.split("=")
                logger.debug("arg={} parts={} len(parts)={}", arg, parts, len(parts))
                if len(parts) == 2:
                    args_dict.update({parts[0]: parts[1]})

        update = {
            "general": {
                "stream": kwargs.get("STREAM"),
                "realization": kwargs.get("ENSMBR"),
                "times": {
                    "basetime": ecflow2datetime_string(kwargs.get("DTG")),
                    "validtime": ecflow2datetime_string(kwargs.get("DTG")),
                    "basetime_pp": ecflow2datetime_string(kwargs.get("DTGPP")),
                },
            },
            "task": {
                "wrapper": kwargs.get("WRAPPER"),
                "var_name": kwargs.get("VAR_NAME"),
                "args": args_dict,
            },
        }
        config = config.copy(update=update)
        get_task(task_name, config).run()
        logger.info("Finished task {}", task_name)
    else:
        task = EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid)
        scheduler = EcflowServerFromConfig(config)

        # This will also handle call to sys.exit(), i.e. Client._   _exit__ will still be called.
        with EcflowClient(scheduler, task):
            task_name = kwargs.get("TASK_NAME")
            logger.info("Running task {}", task_name)
            args = kwargs.get("ARGS")
            args_dict = {}
            if args != "":
                logger.debug("args={}", args)
                for arg in args.split(";"):
                    parts = arg.split("=")
                    logger.debug("arg={} parts={} len(parts)={}", arg, parts, len(parts))
                    if len(parts) == 2:
                        args_dict.update({parts[0]: parts[1]})

            update = {
                "general": {
                    "stream": kwargs.get("STREAM"),
                    "realization": kwargs.get("ENSMBR"),
                    "times": {
                        "basetime": ecflow2datetime_string(kwargs.get("DTG")),
                        "validtime": ecflow2datetime_string(kwargs.get("DTG")),
                        "basetime_pp": ecflow2datetime_string(kwargs.get("DTGPP")),
                    },
                },
                "task": {
                    "wrapper": kwargs.get("WRAPPER"),
                    "var_name": kwargs.get("VAR_NAME"),
                    "args": args_dict,
                },
            }
            config = config.copy(update=update)
            get_task(task.ecf_task, config).run()
            logger.info("Finished task {}", task_name)
