"""PySurfexExpSetup functionality."""
import sys
from argparse import ArgumentParser
import os
import logging
import experiment
try:
    import surfex
except:
    surfex = None


def surfex_exp_setup():
    kwargs = parse_surfex_script_setup(sys.argv[1:])
    surfex_script_setup(**kwargs)


'''
def toml_load(fname):
    """Load from toml file.

    Using tomlkit to preserve stucture

    Args:
        fname (str): Filename

    Returns:
        _type_: _description_

    """
    fh = open(fname, "r", encoding="utf-8")
    res = tomlkit.parse(fh.read())
    fh.close()
    return res


def toml_dump(to_dump, fname):
    """Dump toml to file.

    Using tomlkit to preserve stucture

    Args:
        to_dump (_type_): _description_
        fname (str): Filename
        mode (str, optional): _description_. Defaults to "w".

    """
    fh = open(fname, mode="w", encoding="utf-8")
    fh.write(tomlkit.dumps(to_dump))
    fh.close()
'''

'''
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
        merged_env = experiment.Configuration.merge_toml_env(merged_env, modification)
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
        merged_env = experiment.Configuration.merge_toml_env(merged_env, modification)
    else:
        logging.warning("File not found %s", toml_file)
    return merged_env
'''

'''
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
                    merged_config = experiment.Configuration.merge_toml_env(hm_exp[block], configuration[block])
                    logging.info("Merged: %s %s", block, str(configuration[block]))
                else:
                    merged_config = hm_exp[block]

                block_config.update({block: merged_config})

            if testbed_configuration is not None:
                if block in testbed_configuration:
                    hm_testbed = experiment.Configuration.merge_toml_env(block_config[block],
                                                                         testbed_configuration[block])
                else:
                    hm_testbed = block_config[block]
                block_config.update({block: hm_testbed})

            if user_settings is not None:
                if not isinstance(user_settings, dict):
                    raise Exception("User settings should be a dict here!")
                if block in user_settings:
                    logging.info("Merge user settings in block %s", block)
                    user = experiment.Configuration.merge_toml_env(block_config[block], user_settings[block])
                    block_config.update({block: user})

        config_files.update({this_config_file: {"toml": block_config}})
    return config_files
'''

'''
def get_config_files(config_files_in, blocks):
    """Get the config files.

    Args:
        config_files (dict): config file and path

    Raises:
        Exception: _description_

    Returns:
        dict: returns a config files dict

    """
    # Check existence of needed config files
    config_files = {}
    for ftype, fname in config_files_in.items():
        if os.path.exists(fname):
            toml_dict = toml_load(fname)
        else:
            raise Exception("No config file found for " + fname)

        config_files.update({
            ftype: {
                "toml": toml_dict,
                "blocks": blocks[ftype]["blocks"]
            }
        })
    return config_files
'''

'''
def merge_to_toml_config_files(config_files, wd, configuration=None, testbed_configuration=None,
                               user_settings=None,
                               write_config_files=True):
    """Merge to toml config files.

    Args:
        config_files (_type_): _description_
        wd (_type_): _description_
        configuration (_type_, optional): _description_. Defaults to None.
        testbed_configuration (_type_, optional): _description_. Defaults to None.
        user_settings (_type_, optional): _description_. Defaults to None.
        write_config_files (bool, optional): _description_. Defaults to True.

    """
    config_files = config_files.copy()
    config_files = experiment.Configuration.merge_config_files_dict(config_files, configuration=configuration,
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
            f_out = open(f_out, mode="w", encoding="utf-8")
            f_out.write(tomlkit.dumps(block_config))
            f_out.close()
'''

'''
def setup_files(wd, exp_name, host, pysurfex, pysurfex_experiment,
                offline_source=None,
                configuration=None,
                configuration_file=None):
    """Set up the files for an experiment.

    Args:
        wd (_type_): _description_
        exp_name (_type_): _description_
        host (_type_): _description_
        offline_source (_type_, optional): _description_. Defaults to None.
        configuration (_type_, optional): _description_. Defaults to None.
        configuration_file (_type_, optional): _description_. Defaults to None.

    Raises:
        Exception: _description_
        Exception: _description_

    """

    logging.info("Setting up for host %s", host)
    exp_dependencies = {}
    # Create needed system files
    system_files = {}
    system_files.update({
        "env_system": "/config/system/" + host + ".toml",
        "env": "config/env/" + host + ".py",
        "env_submit": "config/submit/" + host + ".json",
        "env_server": "config/server/" + host + ".json",
        "input_paths": "config/input_paths/" + host + ".json",
    })

    for key, fname in system_files.items():
        lname = f"{wd}/{fname}"
        gname = f"{pysurfex_experiment}/{fname}"
        if os.path.exists(lname):
            logging.info("Using local host specific file %s as %s", fname, key)
            exp_dependencies.update({key: fname})
        elif os.path.exists(gname):
            logging.info("Using general host specific file %s as %s", fname, key)
            exp_dependencies.update({key: gname})
        else:
            raise FileNotFoundError(f"No host file found for lname={lname} or gname={gname}")

    # Check existence of needed config files
    config_file = wd + "/config/config.toml"
    with open(config_file, mode="r", encoding="utf-8") as file_handler:
        c_files = toml.load(file_handler)["config_files"]
    pysurfex_files = ["config_exp_surfex.toml", "first_guess.yml",
                      "config.yml"]
    c_files = c_files + pysurfex_files
    logging.info("Check out config files %s", str(c_files))

    cc_files = {}
    for c_f in c_files:
        lname = f"{wd}/config/{c_f}"
        gname = f"{pysurfex_experiment}/config/{c_f}"
        if c_f in pysurfex_files:
            gname = f"{pysurfex}/surfex/cfg/{c_f}"
        if os.path.exists(lname):
            logging.info("Using local config file %s", lname)
            cc_files.update({c_f: lname})
        elif os.path.exists(gname):
            logging.info("Using general config file %s", gname)
            cc_files.update({c_f: gname})
        else:
            raise FileNotFoundError(f"No config file found for lname={lname} or gname={gname}")

        # if c_f in pysurfex_files:
        #    c_f_p = "config/" + c_f
        #    if os.path.exists(c_f_p):
        #        logging.info("file %s exists", c_f_p)
        #    else:
        #        c_f_s_p = pysurfex + "surfex/cfg/" + c_f
        #        if os.path.exists(c_f_s_p):
        #            os.symlink(c_f_s_p, c_f_p)
        #        else:
        #            raise Exception(f"Missing surfex file {c_f_s_p}")

    exp_dependencies.update({"config_files": cc_files})
    conf = None
    if configuration is not None:
        logging.info("Using configuration %s", configuration)
        lconf = f"{wd}/config/configurations/{configuration.lower()}.toml"
        gconf = f"{pysurfex_experiment}/config/configurations/{configuration.lower()}.toml"
        if os.path.exists(lconf):
            logging.info("Local configuration file %s", lconf)
            conf = lconf
        elif os.path.exists(gconf):
            logging.info("General configuration file %s", gconf)
            conf = gconf
        else:
            raise Exception

    elif configuration_file is not None:
        logging.info("Using configuration from file %s", configuration_file)
        conf = configuration_file

    if conf is not None:
        if not os.path.exists(conf):
            raise Exception("Can not find configuration " + configuration + " in: " + conf)
        configuration = experiment.ExpFromFiles.toml_load(conf)
    else:
        configuration = None

    # Load config files
    config_files = experiment.ExpFromFiles.get_config_files(exp_dependencies["config_files"], c_files)
    # Merge dicts and write to toml config files
    experiment.ExpFromFiles.merge_to_toml_config_files(config_files, wd, configuration=configuration,
                                                       write_config_files=True)

    exp_dependencies.update({
        "exp_dir": wd,
        "exp_name": exp_name,
        "pysurfex_experiment": pysurfex_experiment,
        "pysurfex": pysurfex,
        "offline_source": offline_source
    })
    exp_dependencies_file = wd + "/exp_dependencies.json"
    json.dump(exp_dependencies, open(exp_dependencies_file, mode="w", encoding="utf-8"), indent=2)
'''


def parse_surfex_script_setup(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Surfex offline setup script")
    parser.add_argument('-exp_name', dest="exp", help="Experiment name", type=str, default=None)
    parser.add_argument('--wd', help="Experiment working directory", type=str, default=None)

    # Setup variables
    parser.add_argument('-rev', dest="rev", help="Surfex experiement source revison", type=str,
                        required=False, default=None)
    parser.add_argument('-experiment', dest="pysurfex_experiment",
                        help="Pysurfex-experiment library", type=str, required=True, default=None)
    parser.add_argument('-offline', dest="offline_source", help="Offline source code", type=str,
                        required=False, default=None)
    parser.add_argument('-host', dest="host", help="Host label for setup files", type=str,
                        required=False, default=None)
    parser.add_argument('--config', help="Config", type=str, required=False, default=None)
    parser.add_argument('--config_file', help="Config file", type=str, required=False, default=None)
    parser.add_argument('--debug', dest="debug", action="store_true", help="Debug information")
    # parser.add_argument('--version', action='version', version=__version__)

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
    if debug is None:
        debug = False
    if debug:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    logging.info("************ PySurfexExpSetup ******************")

    # Setup
    exp_name = kwargs.get("exp")
    wdir = kwargs.get("wd")
    pysurfex = f"{os.path.dirname(surfex.__file__)}/../"
    pysurfex_experiment = kwargs.get("pysurfex_experiment")
    offline_source = kwargs.get("offline_source")
    host = kwargs.get("host")
    if host is None:
        raise Exception("You must set a host")

    config = kwargs.get("config")
    config_file = kwargs.get("config_file")

    # Find experiment
    if wdir is None:
        wdir = os.getcwd()
        logging.info("Setting current working directory as WD: %s", wdir)
    if exp_name is None:
        logging.info("Setting EXP from WD: %s", wdir)
        exp_name = wdir.split("/")[-1]
        logging.info("EXP = %s", exp_name)

    if offline_source is None:
        logging.warning("No offline soure code set. Assume existing binaries")

    exp_dependencies = experiment.ExpFromFiles.setup_files(wdir, exp_name, host, pysurfex,
                                                           pysurfex_experiment,
                                                           offline_source=offline_source)

    experiment.ExpFromFiles.write_exp_config(exp_dependencies, configuration=config,
                                             configuration_file=config_file)

    exp_dependencies = experiment.ExpFromFiles.setup_files(wdir, exp_name, host, pysurfex,
                                                           pysurfex_experiment,
                                                           offline_source=offline_source,
                                                           talk=False)

    exp_dependencies_file = wdir + "/exp_dependencies.json"
    experiment.ExpFromFiles.dump_exp_dependencies(exp_dependencies, exp_dependencies_file)
