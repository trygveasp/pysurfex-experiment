import sys
import scheduler
import experiment
from argparse import ArgumentParser
from datetime import datetime
import os
import shutil


def parse_surfex_script(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Surfex offline run script")
    parser.add_argument('action', type=str, help="Action", choices=["start", "prod", "continue", "testbed",
                                                                    "install", "climate", "co"])
    parser.add_argument('-exp_name', dest="exp", help="Experiment name", type=str, default=None)
    parser.add_argument('--wd', help="Experiment working directory", type=str, default=None)

    parser.add_argument('-dtg', help="DateTimeGroup (YYYYMMDDHH)", type=str, required=False, default=None)
    parser.add_argument('-dtgend', help="DateTimeGroup (YYYYMMDDHH)", type=str, required=False, default=None)
    parser.add_argument('--suite', type=str, default="surfex", required=False, help="Type of suite definition")
    parser.add_argument('--stream', type=str, default=None, required=False, help="Stream")

    # co
    parser.add_argument("--file", type=str, default=None, required=False, help="File to checkout")

    parser.add_argument('--debug', dest="debug", action="store_true", help="Debug information")
    parser.add_argument('--version', action='version', version=experiment.__version__)

    if len(argv) == 0:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def surfex_script(**kwargs):

    debug = False
    if "debug" in kwargs:
        debug = kwargs["debug"]

    action = kwargs["action"]
    exp = None
    if "exp" in kwargs:
        exp = kwargs["exp"]

    # Co
    file = kwargs["file"]

    # Others
    dtg = kwargs["dtg"]
    dtgend = kwargs["dtgend"]
    suite = kwargs["suite"]

    begin = True
    if "begin" in kwargs:
        begin = kwargs["begin"]

    wd = None
    if "wd" in kwargs:
        wd = kwargs["wd"]

    # Find experiment
    if wd is None:
        wd = os.getcwd()
        print("Setting current working directory as WD: " + wd)
    if exp is None:
        print("Setting EXP from WD:" + wd)
        exp = wd.split("/")[-1]
        print("EXP = " + exp)

    libs = ["surfex", "scheduler", "experiment"]
    for lib in libs:
        if os.path.exists(wd + "/" + lib):
            sys.path.insert(0, wd + "/" + lib)
            if debug:
                print("Set first in system path: ", wd + "/" + lib)

    if "action" == "mon":
        # TODO

        raise NotImplementedError
    elif action == "co":
        if file is None:
            raise Exception("Checkout requires a file (--file)")
        experiment.ExpFromFiles(exp, wd).checkout(file)
    else:
        # Some kind of start
        if action == "start" and dtg is None:
            raise Exception("You must provide -dtg to start a simulation")
        elif action == "climate":
            if dtg is None:
                dtg = "2008061600"
            if suite is not None and suite != "climate":
                raise Exception("Action was climate but you also specified a suite not being climate: " + suite)
            suite = "climate"
        elif action == "testbed":
            if dtg is None:
                dtg = "2008061600"
            if suite is not None and suite != "testbed":
                raise Exception("Action was climate but you also specified a suite not being testbed: " + suite)
            suite = "testbed"
        elif action == "install":
            if dtg is None:
                dtg = "2008061600"
            suite = "Makeup"

        updated_progress = {}
        updated_progress_pp = {}
        if dtg is not None:
            updated_progress.update({"DTG": dtg})
        if dtgend is not None:
            updated_progress.update({"DTGEND": dtgend})
        if action == "start":
            if dtg is None:
                raise Exception("No DTG was provided!")
            updated_progress.update({"DTG": dtg})
            updated_progress.update({"DTGBEG": dtg})
            if dtgend is not None:
                updated_progress.update({"DTGEND": dtgend})
            else:
                updated_progress.update({"DTGEND": dtg})
            updated_progress_pp.update({"DTGPP": dtg})

        progress_file = wd + "/progress.json"
        progress_pp_file = wd + "/progressPP.json"

        if action.lower() == "prod" or action.lower() == "continue":
            progress = experiment.ProgressFromFile(progress_file, progress_pp_file)
            if dtgend is not None:
                progress.dtgend = datetime.strptime(dtgend, "%Y%m%d%H")
        else:
            progress = experiment.Progress(updated_progress, updated_progress_pp)

        # Update progress
        progress.save(progress_file, progress_pp_file, indent=2)

        # Set experiment from files. Should be existing now after setup
        exp_dependencies_file = wd + "/paths_to_sync.json"
        sfx_exp = experiment.ExpFromFiles(exp_dependencies_file, debug=debug)
        system = sfx_exp.system

        data0 = system.get_var("SFX_EXP_DATA", "0")
        lib0 = system.get_var("SFX_EXP_LIB", "0")
        logfile = data0 + "/ECF.log"

        # Create exp scheduler json file
        sfx_exp.write_scheduler_info(logfile)

        # Create LIB0 and copy init run if WD != lib0
        if wd.rstrip("/") != lib0.rstrip("/"):
            ecf_init_run = lib0 + "/ecf/InitRun.py"
            dirname = os.path.dirname(ecf_init_run)
            # print(dirname)
            dirs = dirname.split("/")
            # print(dirs)
            if len(dirs) > 1:
                p = "/"
                for d in dirs[1:]:
                    p = p + str(d)
                    # print(p)
                    os.makedirs(p, exist_ok=True)
                    p = p + "/"
            shutil.copy2(wd + "/ecf/InitRun.py", ecf_init_run)

        # Create the scheduler
        env_server = sfx_exp.wd + "/Env_server"
        my_scheduler = scheduler.EcflowServerFromFile(env_server, logfile)

        if debug:
            print(__file__, "Creating def file")

        # Create and start the suite
        def_file = data0 + "/" + suite + ".def"

        defs = experiment.get_defs(sfx_exp, system, progress, suite, debug=debug)
        defs.save_as_defs(def_file)
        my_scheduler.start_suite(defs.suite_name, def_file, begin=begin)


def parse_update_config(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Update Surfex offline configuration")
    parser.add_argument('-exp_name', dest="exp", help="Experiment name", type=str, default=None)
    parser.add_argument('--wd', help="Experiment working directory", type=str, default=None)
    parser.add_argument('--debug', dest="debug", action="store_true", help="Debug information")
    parser.add_argument('--version', action='version', version=experiment.__version__)

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def update_config(**kwargs):

    debug = False
    if "debug" in kwargs:
        debug = kwargs["debug"]

    exp = None
    if "exp" in kwargs:
        exp = kwargs["exp"]

    wd = None
    if "wd" in kwargs:
        wd = kwargs["wd"]

    # Find experiment
    if wd is None:
        wd = os.getcwd()
        print("Setting current working directory as WD: " + wd)
    if exp is None:
        print("Setting EXP from WD:" + wd)
        exp = wd.split("/")[-1]
        print("EXP = " + exp)

    # Set experiment from files. Should be existing now after setup
    exp_dependencies_file = wd + "/paths_to_sync.json"
    sfx_exp = experiment.ExpFromFiles(exp_dependencies_file, debug=debug)
    system = sfx_exp.system

    data0 = system.get_var("SFX_EXP_DATA", "0")
    logfile = data0 + "/ECF.log"

    # Create exp scheduler json file
    sfx_exp.write_scheduler_info(logfile)
