"""PySurfexExpSetup functionality."""
import sys
from argparse import ArgumentParser
import json
import os
import logging
import tomlkit
import toml
import shutil
import collections
import copy
import os
import subprocess

# TODO make this a file general for all cli
__version__ = "0.0.1-dev"


def toml_load(fname):
    """Load from toml file.

    Using tomlkit to preserve stucture

    Args:
        fname (str): Filename

    Returns:
        _type_: _description_

    """
    fh = open(fname, "r")
    res = tomlkit.parse(fh.read())
    fh.close()
    return res


def toml_dump(to_dump,  fname, mode="w"):
    """Dump toml to file.

    Using tomlkit to preserve stucture

    Args:
        to_dump (_type_): _description_
        fname (str): Filename
        mode (str, optional): _description_. Defaults to "w".

    """
    fh = open(fname, mode)
    fh.write(tomlkit.dumps(to_dump))
    fh.close()


def flatten(d, sep="#"):
    """Flatten the setting to get member setting.

    Args:
        d (_type_): _description_
        sep (str, optional): Separator. Defaults to "#".

    Returns:
        _type_: _description_

    """
    obj = collections.OrderedDict()

    def recurse(t, parent_key=""):
        if isinstance(t, list):
            for i in range(len(t)):
                recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t, dict):
            for k, v in t.items():
                recurse(v, parent_key + sep + k if parent_key else k)
        else:
            obj[parent_key] = t

    recurse(d)
    return obj


def deep_update(source, overrides):
    """Update a nested dictionary or similar mapping.
   
    Modify ``source`` in place.

    Args:
        source (_type_): _description_
        overrides (_type_): _description_

    Returns:
        _type_: _description_

    """
    for key, value in overrides.items():
        if isinstance(value, collections.Mapping) and value:
            returned = deep_update(source.get(key, {}), value)
            source[key] = returned
        else:
            override = overrides[key]

            source[key] = override

    return source


def merge_toml_env(old_env, mods):
    """Merge the dicts from toml by a deep update.

    Args:
        old_env (_type_): _description_
        mods (_type_): _description_

    Returns:
        _type_: _description_

    """
    return deep_update(old_env, mods)


def merge_toml_env_from_files(toml_files):
    """Read in dicts from toml files and merge them.

    Args:
        toml_files (list): Full path or relative path of toml files

    Returns:
        _type_: _description_

    """
    logging.debug("Merge toml files %s ", str(toml_files))
    merged_env = {}
    for toml_file in toml_files:
        modification = merge_toml_env_from_file(toml_file)
        merged_env = merge_toml_env(merged_env, modification)
    return merged_env


def merge_toml_env_from_file(toml_file):
    """Merge dicts from one toml file.

    Args:
        toml_file (str): Full path or relative path of toml file

    Returns:
        _type_: _description_

    """
    merged_env = {}
    if os.path.exists(toml_file):
        logging.debug("merge_toml_env_from_file: %s", toml_file)
        modification = toml_load(toml_file)
        merged_env = merge_toml_env(merged_env, modification)
    else:
        logging.warning("File not found %s", toml_file)
    return merged_env


def merge_config_files_dict(config_files, configuration=None, testbed_configuration=None,
                            user_settings=None):
    """Merge config files dicts.

    Args:
        config_files (_type_): _description_
        configuration (_type_, optional): _description_. Defaults to None.
        testbed_configuration (_type_, optional): _description_. Defaults to None.
        user_settings (_type_, optional): _description_. Defaults to None.

    Raises:
        Exception: _description_

    Returns:
        _type_: _description_

    """
    for this_config_file in config_files:
        hm_exp = config_files[this_config_file]["toml"].copy()

        block_config = tomlkit.document()
        if configuration is not None:
            f = this_config_file.split("/")[-1]
            if f == "config_exp.toml":
                block_config.add(tomlkit.comment("\n# SURFEX experiment configuration file\n#"))

        for block in config_files[this_config_file]["blocks"]:
            if configuration is not None:
                if block in configuration:
                    merged_config = merge_toml_env(hm_exp[block], configuration[block])
                    logging.info("Merged: %s %s", block, str(configuration[block]))
                else:
                    merged_config = hm_exp[block]

                block_config.update({block: merged_config})

            if testbed_configuration is not None:
                if block in testbed_configuration:
                    hm_testbed = merge_toml_env(block_config[block], testbed_configuration[block])
                else:
                    hm_testbed = block_config[block]
                block_config.update({block: hm_testbed})

            if user_settings is not None:
                if type(user_settings) is not dict:
                    raise Exception("User settings should be a dict here!")
                if block in user_settings:
                    logging.info("Merge user settings in block %s", block)
                    user = merge_toml_env(block_config[block], user_settings[block])
                    block_config.update({block: user})

        config_files.update({this_config_file: {"toml": block_config}})
    return config_files


def get_config_files(wd):
    """Get the config files.

    Looks for "/config/config.toml" in wd

    Args:
        wd (str): working directory

    Raises:
        Exception: _description_

    Returns:
        dict: returns a config files dict

    """
    # Check existence of needed config files
    config_file = wd + "/config/config.toml"
    logging.debug("config_file: %s", config_file)
    config = toml.load(open(config_file, "r"))
    c_files = config["config_files"]
    config_files = {}
    for f in c_files:
        lfile = wd + "/config/" + f
        if os.path.exists(lfile):
            toml_dict = toml_load(lfile)
        else:
            raise Exception("No config file found for " + lfile)

        config_files.update({
            f: {
                "toml": toml_dict,
                "blocks": config[f]["blocks"]
            }
        })
    return config_files


def process_merged_settings(merged_settings):
    """Process the settings and split out member settings.

    Args:
        merged_settings (_type_): _description_

    Returns:
        _type_: _description_

    """
    merged_member_settings = {}
    # Write member settings
    members = None
    if "FORECAST" in merged_settings:
        if "ENSMSEL" in merged_settings["FORECAST"]:
            members = list(merged_settings["FORECAST"]["ENSMSEL"])

    member_settings = {}
    if members is not None:
        for mbr in members:
            toml_settings = copy.deepcopy(merged_settings)
            member_dict = get_member_settings(merged_member_settings, mbr)
            toml_settings = merge_toml_env(toml_settings, member_dict)
            member_settings.update({str(mbr): toml_settings})

    return merged_settings, member_settings


def merge_toml_env_from_config_dicts(config_files):
    """Merge the settings in a config dict.

    Args:
        config_files (list): _description_

    Returns:
        _type_: _description_

    """
    logging.debug("config_files: %s", str(config_files))
    merged_env = {}
    for f in config_files:
        # print(f)
        modification = config_files[f]["toml"]
        merged_env = merge_toml_env(merged_env, modification)
    return merged_env


def get_member_settings(d, member, sep="#"):
    """Get the member setting.

    Args:
        d (_type_): _description_
        member (_type_): _description_
        sep (str, optional): _description_. Defaults to "#".

    Returns:
        _type_: _description_

    """
    member_settings = {}
    settings = flatten(d)
    for setting in settings:
        keys = setting.split(sep)
        if len(keys) == 1:
            member3 = "{:03d}".format(int(member))
            val = settings[setting]
            if type(val) is str:
                val = val.replace("@EEE@", member3)

            this_setting = {keys[0]: val}
            member_settings = merge_toml_env(member_settings, this_setting)
        else:
            this_member = int(keys[-1])
            keys = keys[:-1]
            if this_member == member:
                this_setting = settings[setting]
                for key in reversed(keys):
                    this_setting = {key: this_setting}

                member_settings = merge_toml_env(member_settings, this_setting)
    return member_settings


def merge_to_toml_config_files(config_files, wd, configuration=None, testbed_configuration=None,
                               user_settings=None,
                               write_config_files=True):

    config_files = config_files.copy()
    config_files = merge_config_files_dict(config_files, configuration=configuration,
                                           testbed_configuration=testbed_configuration,
                                           user_settings=user_settings)

    for f in config_files:
        this_config_file = "config/" + f

        block_config = config_files[f]["toml"]
        if write_config_files:
            f_out = wd + "/" + this_config_file
            dirname = os.path.dirname(f_out)
            dirs = dirname.split("/")
            if len(dirs) > 1:
                p = "/"
                for d in dirs[1:]:
                    p = p + str(d)
                    os.makedirs(p, exist_ok=True)
                    p = p + "/"
            f_out = open(f_out, "w")
            f_out.write(tomlkit.dumps(block_config))


def setup_files(wd, exp_name, host, revision, pysurfex_experiment, pysurfex, offline_source=None,
                configuration=None,
                configuration_file=None):
    """Set up the files for an experiment.

    Args:
        wd (_type_): _description_
        exp_name (_type_): _description_
        host (_type_): _description_
        revision (_type_): _description_
        pysurfex_experiment (_type_): _description_
        pysurfex (_type_): _description_
        offline_source (_type_, optional): _description_. Defaults to None.
        configuration (_type_, optional): _description_. Defaults to None.
        configuration_file (_type_, optional): _description_. Defaults to None.

    Raises:
        Exception: _description_
        Exception: _description_

    """
    paths_to_sync = {
        "revision": revision,
        "pysurfex_experiment": pysurfex_experiment,
        "offline_source": offline_source,
        "pysurfex": pysurfex,
        "experiment_is_locked": wd + "/experiment_is_locked@STREAM@",
        "exp_dir": wd,
        "exp_name": exp_name
    }

    env_system = "Env_system"
    env = "Env"
    env_submit = "Env_submit"
    env_server = "Env_server"
    input_paths = "Env_input_paths"

    # Create needed system files
    system_files = {
        env_system: "",
        env: "",
        env_submit: "",
        env_server: "",
        input_paths: "",
    }
    system_files.update({
        env_system: "config/system/" + host + ".toml",
        env: "config/env/" + host + ".py",
        env_submit: "config/submit/" + host + ".json",
        env_server: "config/server/" + host + ".json",
        input_paths: "config/input_paths/" + host + ".json",
    })

    sfiles = []
    for key in system_files:
        sfiles.append(system_files[key])
    logging.info("Setting up for host %s:%s", host, sfiles)
    fetched = []
    revs = [revision]
    if os.path.abspath(revision) != os.path.abspath(pysurfex_experiment):
        revs = revs + [pysurfex_experiment]
    for key in system_files:
        for rev in revs:
            lfile = wd + "/" + system_files[key]
            rfile = rev + "/" + system_files[key]
            dirname = os.path.dirname(lfile)
            logging.debug("rfile %s lfile %s dirname %s", rfile, lfile, dirname)
            os.makedirs(dirname, exist_ok=True)
            if os.path.exists(lfile):
                if lfile not in fetched:
                    logging.info("System file %s already exists, is not fetched again from %s", 
                                 lfile, rev)
            else:
                if os.path.exists(rfile) and lfile != rfile:
                    logging.debug("Copy %s to %s", rfile, lfile)
                    shutil.copy2(rfile, lfile)
                    fetched.append(lfile)

        target = wd + "/" + key
        lfile = wd + "/" + system_files[key]
        if os.path.islink(target):
            if target not in fetched:
                logging.info("System target file %s already exists", str(target))
        else:
            os.symlink(lfile, target)
            fetched.append(target)

    # Set up pysurfex_experiment
    logging.info("Set up experiment from %s", str(revs))
    config_dirs = ["experiment", "bin", "experiment_scheduler", "experiment_tasks", 
                   "experiment_setup"]
    for rev in revs:
        for cdir in config_dirs:
            os.makedirs(wd + "/" + cdir, exist_ok=True)
            files = [f for f in os.listdir(rev + "/" + cdir)
                     if os.path.isfile(os.path.join(rev + "/" + cdir, f))]
            for f in files:
                lfile = wd + "/" + cdir + "/" + f
                rfile = rev + "/" + cdir + "/" + f
                dirname = os.path.dirname(lfile)
                os.makedirs(dirname, exist_ok=True)
                if not os.path.exists(lfile):
                    if os.path.exists(rfile) and lfile != rfile:
                        shutil.copy2(rfile, lfile)
                        fetched.append(lfile)
                else:
                    if lfile not in fetched:
                        logging.info("%s already exists, is not fetched again from %s", lfile, rev)

    '''
    print("Fetch configuration from pysurfex from ", pysurfex)
    files = ["configuration.py"]
    for f in files:
        lfile = wd + "/experiment/" + f
        rfile = pysurfex + "/surfex/" + f
        if not os.path.exists(lfile):
            if os.path.exists(rfile) and lfile != rfile:
                # print("Copy " + rfile + " to " + lfile)
                shutil.copy2(rfile, lfile)
                fetched.append(lfile)
        else:
            if lfile not in fetched:
                print(lfile + " already exists, is not fetched again from " + pysurfex)
    '''

    # Check existence of needed config files
    config_file = wd + "/config/config.toml"
    os.makedirs(wd + "/config/", exist_ok=True)
    for rev in revs:
        lfile = config_file
        rfile = rev + "/config/config.toml"
        if not os.path.exists(lfile):
            if os.path.exists(rfile) and lfile != rfile:
                logging.debug("Copy %s to %s", rfile, lfile)
                shutil.copy2(rfile, lfile)
                fetched.append(lfile)
        else:
            if lfile not in fetched:
                logging.info("%s already exists, is not fetched again from %s", lfile, rev)

    c_files = toml.load(open(config_file, "r"))["config_files"]
    c_files = c_files + ["first_guess.yml", "config.yml"]
    logging.info("Check out config files %s", str(c_files))
    for rev in [pysurfex] + revs:
        config_files = []
        os.makedirs(wd + "/config/", exist_ok=True)
        for f in c_files:
            lfile = wd + "/config/" + f
            config_files.append(lfile)
            os.makedirs(wd + "/config", exist_ok=True)
            logging.debug("rev %s", rev)
            logging.debug("self.pysurfex %s", pysurfex)
            if rev == revision:
                rfile = rev + "/config/" + f
            else:
                if f == "config_exp.toml" or f == "config_exp_surfex.toml" \
                                          or f == "first_guess.yml" \
                                          or f == "config.yml":
                    rfile = pysurfex + "/surfex/cfg/" + f
                else:
                    rfile = rev + "/config/" + f

            if not os.path.exists(lfile):
                logging.debug("rfile=%s lfile=%s", rfile, lfile)
                if os.path.exists(rfile) and lfile != rfile:
                    logging.debug("Copy " + rfile + " -> " + lfile)
                    shutil.copy2(rfile, lfile)
                    fetched.append(lfile)
            else:
                if lfile not in fetched:
                    logging.info("Config file %s already exists, is not fetched again from %s",
                                 lfile, rev)

    for f in c_files:
        lfile = wd + "/config/" + f
        if not os.path.exists(lfile):
            raise Exception("Config file not found ", lfile)

    conf = None
    if configuration is not None:
        logging.info("Using configuration %s", configuration)
        conf = wd + "/config/configurations/" + configuration.lower() + ".toml"
        if not os.path.exists(conf):
            conf = pysurfex_experiment + "/config/configurations/" + configuration.lower() + ".toml"
        logging.info("Configuration file %s", configuration_file)
    elif configuration_file is not None:
        logging.info("Using configuration from file %s", configuration_file)
        conf = configuration_file

    config_files = get_config_files(wd)
    if conf is not None:
        if not os.path.exists(conf):
            raise Exception("Can not find configuration " + configuration + " in: " + conf)
        configuration = toml_load(conf)
        merge_to_toml_config_files(config_files, wd, configuration=configuration, 
                                  write_config_files=True)

    logging.info("Set up domains from %s", str(revs))
    dirs = ["config/domains"]
    # Copy dirs
    for dir_path in dirs:
        os.makedirs(wd + "/" + dir_path, exist_ok=True)
        for rev in revs:
            files = [f for f in os.listdir(rev + "/" + dir_path)
                     if os.path.isfile(os.path.join(rev + "/" + dir_path, f))]

            for f in files:
                lfile = wd + "/" + dir_path + "/" + f
                rfile = rev + "/" + dir_path + "/" + f
                if not os.path.exists(lfile):
                    logging.debug("rfile=%s, lfile=%s", rfile, lfile)
                    if os.path.exists(rfile) and lfile != rfile:
                        logging.debug("Copy %s -> %s", rfile, lfile)
                        shutil.copy2(rfile, lfile)
                        fetched.append(lfile)
                else:
                    if lfile not in fetched:
                        logging.info("%s already exists, is not fetched again from %s", lfile, rev)

    logging.info("Set up submission exceptions from %s", str(revs))
    for rev in revs:
        # ECF_submit exceptions
        f = "config/submit/submission.json"
        lfile = wd + "/" + f
        rfile = rev + "/" + f
        if not os.path.exists(lfile):
            if os.path.exists(rfile) and lfile != rfile:
                logging.info("Copy %s -> %s", rfile, lfile)
                shutil.copy2(rfile, lfile)
                fetched.append(lfile)
        else:
            if lfile not in fetched:
                logging.info("%s already exists, is not fetched again from %s", lfile, rev)

    logging.info("Set up ecflow default containers from %s", str(revs))
    # Init run
    files = ["ecf/InitRun.py", "ecf/default.py"]
    os.makedirs(wd + "/ecf", exist_ok=True)
    for rev in revs:
        for f in files:
            lfile = wd + "/" + f
            rfile = rev + "/" + f
            if not os.path.exists(lfile):
                logging.debug("rfile=%s lfile=%s", rfile, lfile)
                if os.path.exists(rfile) and lfile != rfile:
                    logging.debug("Copy %s -> %s", rfile, lfile)
                    shutil.copy2(rfile, lfile)
                    fetched.append(lfile)
            else:
                if lfile not in fetched:
                    logging.info("%s already exists, is not fetched again from %s", lfile, rev)

    logging.info("Copy namelists from %s", revs)
    exp_dirs = ["nam"]
    for rev in revs:
        for exp_dir in exp_dirs:
            os.makedirs(wd + "/" + exp_dir, exist_ok=True)
            rdir = rev + "/" + exp_dir
            ldir = wd + "/" + exp_dir
            files = [f for f in os.listdir(rev + "/" + exp_dir)
                     if os.path.isfile(os.path.join(rev + "/" + exp_dir, f))]
            for f in files:
                lfile = ldir + "/" + f
                rfile = rdir + "/" + f
                dirname = os.path.dirname(lfile)
                os.makedirs(dirname, exist_ok=True)
                if not os.path.exists(lfile):
                    if os.path.exists(rfile) and lfile != rfile:
                        logging.debug("Copy %s -> %s", rfile, lfile)
                        # shutil.copytree(rdir, ldir)
                        shutil.copy2(rfile, lfile)
                        fetched.append(lfile)
                else:
                    if lfile not in fetched:
                        logging.info("%s already exists, is not fetched again from %s", lfile, rev)

    # Set up offline
    revs = [revision]
    config_dirs = ["offline"]
    for rev in revs:
        for cdir in config_dirs:
            logging.debug("%s/%s %s/%s", rev, cdir, wd, cdir)
            if os.path.exists(rev + "/" + cdir):
                if not os.path.exists(wd + "/" + cdir):
                    logging.info("Copy source code " + rev + "/" + cdir + " -> " + wd + "/" + cdir)
                    shutil.copytree(rev + "/" + cdir, wd + "/" + cdir)
                else:
                    logging.info(wd + "/" + cdir + " already exists, is not fetched again from " +
                                 rev)

    json.dump(paths_to_sync, open(wd + "/paths_to_sync.json", "w"), indent=2)


def parse_surfex_script_setup(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Surfex offline setup script")
    parser.add_argument('-exp_name', dest="exp", help="Experiment name", type=str, default=None)
    parser.add_argument('--wd', help="Experiment working directory", type=str, default=None)

    parser.add_argument('-dtg', help="DateTimeGroup (YYYYMMDDHH)", type=str, required=False,
                        default=None)
    parser.add_argument('-dtgend', help="DateTimeGroup (YYYYMMDDHH)", type=str, required=False,
                        default=None)
    parser.add_argument('--suite', type=str, default="surfex", required=False,
                        help="Type of suite definition")
    parser.add_argument('--stream', type=str, default=None, required=False, help="Stream")

    # Setup variables
    parser.add_argument('-rev', dest="rev", help="Surfex experiement source revison", type=str,
                        required=False, default=None)
    parser.add_argument('-surfex', dest="pysurfex", help="Pysurfex library", type=str,
                        required=False, default=None)
    parser.add_argument('-experiment', dest="pysurfex_experiment",
                        help="Pysurfex-experiment library", type=str, required=False, default=None)
    parser.add_argument('-offline', dest="offline_source", help="Offline source code", type=str,
                        required=False, default=None)
    parser.add_argument('-host', dest="host", help="Host label for setup files", type=str,
                        required=False, default=None)
    parser.add_argument('--config', help="Config", type=str, required=False, default=None)
    parser.add_argument('--config_file', help="Config file", type=str, required=False, default=None)
    parser.add_argument('--debug', dest="debug", action="store_true", help="Debug information")
    parser.add_argument('--version', action='version', version=__version__)

    if len(argv) == 0:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def surfex_script_setup(**kwargs):
    """Do experiment setup.

    Raises:
        Exception: _description_
        Exception: _description_
        Exception: _description_
        Exception: _description_

    """
    debug = kwargs.get("debug")
    level = logging.INFO
    if debug is None:
        debug = False
    if debug:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    logging.info("************ PySurfexExpSetup ******************")

    # Setup
    exp_name = kwargs.get("exp")
    wd = kwargs.get("wd")
    rev = kwargs.get("rev")
    pysurfex = kwargs.get("pysurfex")
    pysurfex_experiment = kwargs.get("pysurfex_experiment")
    offline_source = kwargs.get("offline_source")
    host = kwargs.get("host")
    if host is None:
        raise Exception("You must set a host")

    config = kwargs.get("config")
    config_file = kwargs.get("config_file")       

    # Find experiment
    if wd is None:
        wd = os.getcwd()
        logging.info("Setting current working directory as WD: %s", wd)
    if exp_name is None:
        logging.info("Setting EXP from WD: %s", wd)
        exp_name = wd.split("/")[-1]
        logging.info("EXP = %s", exp_name)

    # Copy files to WD from REV
    if rev is None:
        if pysurfex_experiment is None:
            raise Exception("You must set REV or pysurfex_experiment")
        else:
            logging.info("Using %s as rev", pysurfex_experiment)
            rev = pysurfex_experiment
    if pysurfex is None:
        if rev is not None:
            logging.info("Using %s as pysurfex", rev)
            pysurfex = rev
        else:
            raise Exception("pysurfex must be set when rev is not set")
    if pysurfex_experiment is None:
        if rev is not None:
            logging.info("Using %s as pysurfex_experiment", rev)
            pysurfex_experiment = rev
        else:
            raise Exception("pysurfex_experiment must be set when rev is not set")
    if offline_source is None:
        logging.warning("No offline soure code set. Assume existing binaries")

    setup_files(wd, exp_name, host,  rev, pysurfex_experiment, pysurfex,
                offline_source=offline_source,
                configuration=config, configuration_file=config_file)

def init_run_from_file(system_file, exp_dependencies, stream_nr=None):
    """Call init_run from experiment files.

    Args:
        system_file (str): File with system
        exp_dependencies (str): File with experiment paths 
        stream_nr (int, optional): stream. Defaults to None.

    Raises:
        FileNotFoundError: _description_
        FileNotFoundError: _description_

    """
    if os.path.exists(system_file):
        system = json.load(open(system_file, "r"))
    else:
        raise FileNotFoundError(system_file)
    if os.path.exists(exp_dependencies):
        paths_to_sync = json.load(open(exp_dependencies, "r"))
    else:
        raise FileNotFoundError(exp_dependencies)
    init_run(system, paths_to_sync, stream_nr=stream_nr)


def init_run(system, paths_to_sync, stream_nr=None):
    """Sync experiment to scratch and between machines.

    Args:
        system (dict): System dictionary with all hosts.
        paths_to_sync (dict): Paths to needed components.
        stream_nr (int, optional): stream. Defaults to None.

    Raises:
        Exception: _description_
        Exception: _description_
        Exception: _description_
        Exception: _description_
        Exception: _description_
        Exception: _description_
        Exception: _description_

    """
    rev = paths_to_sync["revision"]
    pysurfex_experiment = paths_to_sync["pysurfex_experiment"]
    offline_source = paths_to_sync["offline_source"]
    pysurfex = paths_to_sync["pysurfex"]
    experiment_is_locked_file = paths_to_sync["experiment_is_locked"]
    wd = paths_to_sync["exp_dir"]

    if stream_nr is None:
        stream_nr = ""
    else:
        stream_nr = str(stream_nr)

    experiment_is_locked_file = experiment_is_locked_file.replace("@STREAM@", stream_nr)
    if os.path.exists(experiment_is_locked_file):
        experiment_is_locked = True
    else:
        experiment_is_locked = False

    rsync = system["0"]["RSYNC"].replace("@STREAM@", stream_nr)
    lib0 = system["0"]["SFX_EXP_LIB"].replace("@STREAM@", stream_nr)
    host_name0 = ""

    excludes = " --exclude=.git --exclude=.vscode --exclude=.github --exclude=coverage" + \
               " --exclude=.cache --exclude=__pycache__ --exclude='*.pyc'"
    # Sync pysurfex_experiment to LIB0
    if not experiment_is_locked:
        dirs = ["experiment", "nam", "toml", "config", "ecf", "experiment_tasks", 
                "experiment_scheduler", "experiment_setup"]
        for d in dirs:
            os.makedirs(f"{lib0}/{d}", exist_ok=True)
            cmd = f"{rsync} {pysurfex_experiment}/{d} {host_name0}{lib0}/{d} {excludes}"
            logging.info(cmd)
            ret = subprocess.call(cmd, shell=True)
            if ret != 0:
                raise Exception(cmd + " failed!")
    else:
        logging.info("Not resyncing " + pysurfex_experiment + " as experiment is locked")

    # Sync pysurfex to LIB0
    if not experiment_is_locked:
        dirs = ["surfex"]
        for d in dirs:
            os.makedirs(lib0 + "/" + d, exist_ok=True)
            cmd = f"{rsync} {pysurfex}/{d} {host_name0}{lib0}/{d} {excludes}"
            logging.info(cmd)
            ret = subprocess.call(cmd, shell=True)
            if ret != 0:
                raise Exception(cmd + " failed!")
    else:
        logging.info("Not resyncing " + pysurfex_experiment + " as experiment is locked")

    # Sync REV to LIB0
    if not experiment_is_locked:
        if rev != wd:
            cmd = f"{rsync} {rev}/* {host_name0}{lib0}/. {excludes}"
            logging.info(cmd)
            ret = subprocess.call(cmd, shell=True)
            if ret != 0:
                raise Exception(cmd + " failed!")
        else:
            logging.info("REV == WD. No syncing needed")
    else:
        logging.info("Not resyncing REV as experiment is locked")

    # Sync offline source code to LIB0
    if not experiment_is_locked:
        if offline_source is not None:
            if rev != wd:
                cmd = f"{rsync} {offline_source} {host_name0}{lib0}/offline {excludes}"
                logging.info(cmd)
                ret = subprocess.call(cmd, shell=True)
                if ret != 0:
                    raise Exception(cmd + " failed!")
            else:
                logging.info("REV == WD. No syncing needed")
    else:
        logging.info("Not resyncing REV as experiment is locked")

    # Sync WD to LIB
    # Always sync WD unless it is not same as SFX_EXP_LIB
    if wd != lib0:
        cmd = f"{rsync} {wd}/* {host_name0}{lib0}/. {excludes}"
        logging.info(cmd)
        ret = subprocess.call(cmd, shell=True)
        if ret != 0:
            raise Exception

    # TODO sync LIB to stream

    host_label = []
    for h in system:
        host_label.append(system[h]["HOSTNAME"])

    # Sync HM_LIB beween hosts
    # TODO sync streams
    if len(host_label) > 1:
        for host in range(1, len(host_label)):
            host = str(host)
            print("Syncing to HOST" + host + " with label " + host_label[int(host)])
            rsync = system[host]["RSYNC"].replace("@STREAM@", stream_nr)
            libn = system[host]["SFX_EXP_LIB"].replace("@STREAM@", stream_nr)
            datan = system[host]["SFX_EXP_DATA"].replace("@STREAM@", stream_nr)
            mkdirn = system[host]["MKDIR"].replace("@STREAM@", stream_nr)
            host_namen = system[host]["LOGIN_HOST"].replace("@STREAM@", stream_nr)
            sync_data = system[host]["SYNC_DATA"]

            if sync_data:
                # libn = system.get_var("SFX_EXP_LIB", host, stream=stream)
                # datan = system.get_var("SFX_EXP_DATA", host, stream=stream)
                # mkdirn = system.get_var("MKDIR", host, stream=stream)
                # host_namen = system.get_var("HOST_NAME", host, stream=stream)
                ssh = ""
                if host_namen != "":
                    ssh = "ssh " + os.environ["USER"] + "@" + host_namen
                    host_namen = os.environ["USER"] + "@" + host_namen + ":"

                cmd = mkdirn + " " + datan
                logging.info(cmd)
                cmd = mkdirn + " " + libn
                if ssh != "":
                    cmd = ssh + " \"" + mkdirn + " " + libn + "\""
                logging.info(cmd)
                subprocess.call(cmd, shell=True)
                if ret != 0:
                    raise Exception
                cmd = rsync + " " + host_name0 + lib0 + "/ " + host_namen + libn + \
                                " --exclude=.git --exclude=.vscode --exclude=.github --exclude=coverage --exclude=.cache --exclude=.idea --exclude=__pycache__ --exclude='*.pyc'"
                logging.info(cmd)
                subprocess.call(cmd, shell=True)
                if ret != 0:
                    raise Exception
            else:
                logging.warn("Data sync to " + host_namen + " disabled")

    logging.info("Lock experiment")
    fh = open(experiment_is_locked_file, "w")
    fh.write("Something from git?")
    fh.close()
    logging.info("Finished syncing")
