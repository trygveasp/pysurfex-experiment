"""PySurfexExpSetup functionality."""
import sys
from argparse import ArgumentParser
import os
import json
import logging
# import collections
# import copy
import tomlkit
import toml
try:
    import surfex
except:
    surfex = None

# TODO make this a file general for all cli
__version__ = "0.0.1-dev"


def surfex_exp_setup():
    kwargs = parse_surfex_script_setup(sys.argv[1:])
    surfex_script_setup(**kwargs)


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
            for i in enumerate(t):
                recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t, dict):
            for k, v in t.items():
                recurse(v, parent_key + sep + k if parent_key else k)
        else:
            obj[parent_key] = t

    recurse(d)
    return obj
'''


'''
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
'''

'''
def merge_toml_env(old_env, mods):
    """Merge the dicts from toml by a deep update.

    Args:
        old_env (_type_): _description_
        mods (_type_): _description_

    Returns:
        _type_: _description_

    """
    return deep_update(old_env, mods)
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
                if not isinstance(user_settings, dict):
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
    with open(config_file, mode="r", encoding="utf-8") as file_handler:
        config = toml.load(file_handler)
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


'''
def process_merged_settings(merged_settings):
    """Process the settings and split out member settings.

    Args:
        merged_settings (dict): dict with all settings

    Returns:
        (dict, dict): General config, member_config

    """
    # Write member settings
    members = None
    if "FORECAST" in merged_settings:
        if "ENSMSEL" in merged_settings["FORECAST"]:
            members = list(merged_settings["FORECAST"]["ENSMSEL"])

    logging.debug("Merged settings %s", merged_settings)
    merged_member_settings = {}
    if "EPS" in merged_settings:
        if "MEMBER_SETTINGS" in merged_settings["EPS"]:
            merged_member_settings = copy.deepcopy(merged_settings["EPS"]["MEMBER_SETTINGS"])
            logging.debug("Found member settings %s", merged_member_settings)
            del merged_settings["EPS"]["MEMBER_SETTINGS"]

    member_settings = {}
    if members is not None:
        for mbr in members:
            toml_settings = copy.deepcopy(merged_settings)
            logging.debug("member: %s merged_member_dict: %s", str(mbr), merged_member_settings)
            member_dict = get_member_settings(merged_member_settings, mbr)
            logging.debug("member_dict: %s", member_dict)
            toml_settings = merge_toml_env(toml_settings, member_dict)
            member_settings.update({str(mbr): toml_settings})

    return merged_settings, member_settings
'''


'''
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
'''

''''''
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
        logging.debug("Keys: %s", str(keys))
        if len(keys) == 1:
            member3 = f"{int(member):03d}"
            val = settings[setting]
            if isinstance(val, str):
                val = val.replace("@EEE@", member3)

            this_setting = {keys[0]: val}
            member_settings = merge_toml_env(member_settings, this_setting)
        else:
            this_member = int(keys[-1])
            keys = keys[:-1]
            logging.debug("This member: %s member=%s Keys=%s", str(this_member), str(member), keys)
            if int(this_member) == int(member):
                this_setting = settings[setting]
                for key in reversed(keys):
                    this_setting = {key: this_setting}

                logging.debug("This setting: %s", str(this_setting))
                member_settings = merge_toml_env(member_settings, this_setting)
                logging.debug("Merged member settings for member %s = %s",
                              str(member), str(member_settings))
    logging.debug("Finished member settings for member %s = %s", str(member), str(member_settings))
    return member_settings
''''''


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
            f_out = open(f_out, mode="w", encoding="utf-8")
            f_out.write(tomlkit.dumps(block_config))
            f_out.close()


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
    for sfile in system_files.values():
        sfiles.append(sfile)
    logging.info("Setting up for host %s:%s", host, sfiles)
    fetched = []
    for lname, fname in system_files.items():

        target = wd + "/" + lname
        lfile = wd + "/" + fname
        if os.path.islink(target):
            if target not in fetched:
                logging.info("System target file %s already exists", str(target))
        else:
            os.symlink(lfile, target)
            fetched.append(target)

    # Check existence of needed config files
    config_file = wd + "/config/config.toml"
    with open(config_file, mode="r", encoding="utf-8") as file_handler:
        c_files = toml.load(file_handler)["config_files"]
    pysurfex_files = ["config_exp_surfex.toml", "first_guess.yml",\
                      "config.yml"]
    c_files = c_files + pysurfex_files
    logging.info("Check out config files %s", str(c_files))
    for c_f in c_files:
        if c_f in pysurfex_files:
            c_f_p = "config/" + c_f
            if os.path.exists(c_f_p):
                logging.info("file %s exists", c_f_p)
            else:
                c_f_s_p = pysurfex + "surfex/cfg/" + c_f
                if os.path.exists(c_f_s_p):
                    os.symlink(c_f_s_p, c_f_p)
                else:
                    raise Exception(f"Missing surfex file {c_f_s_p}")

    conf = None
    if configuration is not None:
        logging.info("Using configuration %s", configuration)
        conf = wd + "/config/configurations/" + configuration.lower() + ".toml"
        if not os.path.exists(conf):
            conf = "config/configurations/" +\
            configuration.lower() + ".toml"
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

    exp_dependencies = {
        "exp_dir": wd,
        "exp_name": exp_name,
        "pysurfex_experiment": pysurfex_experiment,
        "pysurfex": pysurfex,
        "offline_source": offline_source
    }
    exp_dependencies_file = wd + "/exp_dependencies.json"
    json.dump(exp_dependencies, open(exp_dependencies_file, mode="w", encoding="UTF-8"))


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

    setup_files(wdir, exp_name, host, pysurfex, pysurfex_experiment,
                offline_source=offline_source,
                configuration=config, configuration_file=config_file)
