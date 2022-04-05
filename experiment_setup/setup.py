import sys
from argparse import ArgumentParser
import json
import os
import tomlkit
import toml
import shutil
import collections
import copy

# TODO make this a file general for all cli
__version__ = "0.0.1-dev"


def toml_load(fname):
    fh = open(fname, "r")
    res = tomlkit.parse(fh.read())
    fh.close()
    return res


def toml_dump(to_dump,  fname, mode="w"):
    fh = open(fname, mode)
    fh.write(tomlkit.dumps(to_dump))
    fh.close()


def flatten(d, sep="#"):

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
    """
    Update a nested dictionary or similar mapping.
    Modify ``source`` in place.
    """
    for key, value in overrides.items():
        if isinstance(value, collections.Mapping) and value:
            returned = deep_update(source.get(key, {}), value)
            # print("Returned:", key, returned)
            source[key] = returned
        else:
            override = overrides[key]
            # print("Override:", key, override)

            source[key] = override

    return source


def merge_toml_env(old_env, mods):
    # print(mods)
    return deep_update(old_env, mods)


def merge_toml_env_from_files(toml_files):
    merged_env = {}
    for toml_file in toml_files:
        if os.path.exists(toml_file):
            modification = toml_load(toml_file)
            merged_env = merge_toml_env(merged_env, modification)
        else:
            print("WARNING: File not found " + toml_file)
    return merged_env


def merge_toml_env_from_file(toml_file):
    merged_env = {}
    if os.path.exists(toml_file):
        # print(toml_file)
        modification = toml_load(toml_file)
        merged_env = merge_toml_env(merged_env, modification)
    else:
        print("WARNING: File not found " + toml_file)
    return merged_env


def merge_config_files_dict(config_files, configuration=None, testbed_configuration=None,
                            user_settings=None):

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
                    print("Merged: ", block, configuration[block])
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
                    print("Merge user settings in block " + block)
                    user = merge_toml_env(block_config[block], user_settings[block])
                    block_config.update({block: user})

        config_files.update({this_config_file: {"toml": block_config}})
    return config_files


def get_config_files(wd, debug=False):

    # Check existence of needed config files
    config_file = wd + "/config/config.toml"
    if debug:
        print(__file__, "config_file:", config_file)
    config = toml.load(open(config_file, "r"))
    c_files = config["config_files"]
    config_files = {}
    for f in c_files:
        lfile = wd + "/config/" + f
        if os.path.exists(lfile):
            # print("lfile", lfile)
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

    merged_member_settings = {}
    # Write member settings
    members = None
    if "FORECAST" in merged_settings:
        if "ENSMSEL" in merged_settings["FORECAST"]:
            members = list(merged_settings["FORECAST"]["ENSMSEL"])

    # print(members, type(members), len(members))
    member_settings = {}
    if members is not None:
        for mbr in members:
            toml_settings = copy.deepcopy(merged_settings)
            member_dict = get_member_settings(merged_member_settings, mbr)
            toml_settings = merge_toml_env(toml_settings, member_dict)
            member_settings.update({str(mbr): toml_settings})

    return merged_settings, member_settings


def merge_toml_env_from_config_dicts(config_files):

    merged_env = {}
    for f in config_files:
        # print(f)
        modification = config_files[f]["toml"]
        merged_env = merge_toml_env(merged_env, modification)
    return merged_env


def get_member_settings(d, member, sep="#"):

    member_settings = {}
    settings = flatten(d)
    for setting in settings:
        # print(setting)
        keys = setting.split(sep)
        # print(keys)
        if len(keys) == 1:
            # print(member)
            member3 = "{:03d}".format(int(member))
            val = settings[setting]
            if type(val) is str:
                val = val.replace("@EEE@", member3)

            this_setting = {keys[0]: val}
            # print("This setting", this_setting)
            member_settings = merge_toml_env(member_settings, this_setting)
        else:
            this_member = int(keys[-1])
            keys = keys[:-1]
            # print(keys)
            if this_member == member:
                # print("This is it")
                # print(setting, keys, this_member)

                this_setting = settings[setting]
                for key in reversed(keys):
                    this_setting = {key: this_setting}

                # print(this_setting)
                member_settings = merge_toml_env(member_settings, this_setting)
    return member_settings


def merge_to_toml_config_files(config_files, wd, configuration=None, testbed_configuration=None,
                               user_settings=None,
                               write_config_files=True):

    # print(self.config_files)
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
            f_out = open(f_out, "w")
            f_out.write(tomlkit.dumps(block_config))


def setup_files(wd, exp_name, host, revision, pysurfex_experiment, pysurfex, offline_source=None,
                configuration=None,
                configuration_file=None, debug=False):

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
    print("Setting up for host ", host, ":", sfiles)
    fetched = []
    revs = [revision]
    if os.path.abspath(revision) != os.path.abspath(pysurfex_experiment):
        revs = revs + [pysurfex_experiment]
    for key in system_files:
        for rev in revs:
            lfile = wd + "/" + system_files[key]
            rfile = rev + "/" + system_files[key]
            dirname = os.path.dirname(lfile)
            if debug:
                print(__file__, rfile, lfile)
                print(__file__, dirname)
            os.makedirs(dirname, exist_ok=True)
            if os.path.exists(lfile):
                if lfile not in fetched:
                    print("System file " + lfile + " already exists, is not fetched again from " + rev)
            else:
                if os.path.exists(rfile) and lfile != rfile:
                    if debug:
                        print("Copy " + rfile + " to " + lfile)
                    shutil.copy2(rfile, lfile)
                    fetched.append(lfile)

        target = wd + "/" + key
        lfile = wd + "/" + system_files[key]
        if os.path.islink(target):
            if target not in fetched:
                print("System target file " + target + " already exists")
        else:
            os.symlink(lfile, target)
            fetched.append(target)

    # Set up pysurfex_experiment
    print("Set up experiment from ", revs)
    config_dirs = ["experiment", "bin", "experiment_scheduler", "experiment_tasks"]
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
                        # print("Copy " + rfile + " to " + lfile)
                        shutil.copy2(rfile, lfile)
                        fetched.append(lfile)
                else:
                    if lfile not in fetched:
                        print(lfile + " already exists, is not fetched again from " + rev)

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
                if debug:
                    print("Copy " + rfile + " to " + lfile)
                shutil.copy2(rfile, lfile)
                fetched.append(lfile)
        else:
            if lfile not in fetched:
                print(lfile + " already exists, is not fetched again from " + rev)

    c_files = toml.load(open(config_file, "r"))["config_files"]
    c_files = c_files + ["first_guess.yml", "config.yml"]
    print("Check out config files ", c_files)
    for rev in [pysurfex] + revs:
        config_files = []
        os.makedirs(wd + "/config/", exist_ok=True)
        for f in c_files:
            lfile = wd + "/config/" + f
            config_files.append(lfile)
            os.makedirs(wd + "/config", exist_ok=True)
            if debug:
                print("rev ", rev)
                print("self.pysurfex ", pysurfex)
            if rev == revision:
                rfile = rev + "/config/" + f
            else:
                if f == "config_exp.toml" or f == "config_exp_surfex.toml" or f == "first_guess.yml" or \
                        f == "config.yml":
                    rfile = pysurfex + "/surfex/cfg/" + f
                else:
                    rfile = rev + "/config/" + f

            if not os.path.exists(lfile):
                if debug:
                    print(rfile, lfile)
                if os.path.exists(rfile) and lfile != rfile:
                    if debug:
                        print("Copy " + rfile + " -> " + lfile)
                    shutil.copy2(rfile, lfile)
                    fetched.append(lfile)
            else:
                if lfile not in fetched:
                    print("Config file " + lfile + " already exists, is not fetched again from " + rev)

    for f in c_files:
        lfile = wd + "/config/" + f
        if not os.path.exists(lfile):
            raise Exception("Config file not found ", lfile)

    conf = None
    if configuration is not None:
        print("Using configuration ", configuration)
        conf = wd + "/config/configurations/" + configuration.lower() + ".toml"
        if not os.path.exists(conf):
            conf = pysurfex_experiment + "/config/configurations/" + configuration.lower() + ".toml"
        print("Configuration file ", configuration_file)
    elif configuration_file is not None:
        print("Using configuration from file ", configuration_file)
        conf = configuration_file

    config_files = get_config_files(wd)
    if conf is not None:
        if not os.path.exists(conf):
            raise Exception("Can not find configuration " + configuration + " in: " + conf)
        configuration = toml_load(conf)
        merge_to_toml_config_files(config_files, wd, configuration=configuration, write_config_files=True)

    print("Set up domains from ", revs)
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
                    if debug:
                        print(rfile, lfile)
                    if os.path.exists(rfile) and lfile != rfile:
                        if debug:
                            print("Copy " + rfile + " -> " + lfile)
                        shutil.copy2(rfile, lfile)
                        fetched.append(lfile)
                else:
                    if lfile not in fetched:
                        print(lfile + " already exists, is not fetched again from " + rev)

    print("Set up submission exceptions from ", revs)
    for rev in revs:
        # ECF_submit exceptions
        f = "config/submit/submission.json"
        lfile = wd + "/" + f
        rfile = rev + "/" + f
        if not os.path.exists(lfile):
            if os.path.exists(rfile) and lfile != rfile:
                if debug:
                    print("Copy " + rfile + " -> " + lfile)
                shutil.copy2(rfile, lfile)
                fetched.append(lfile)
        else:
            if lfile not in fetched:
                print(lfile + " already exists, is not fetched again from " + rev)

    print("Set up ecflow default containers from ", revs)
    # Init run
    files = ["ecf/InitRun.py", "ecf/default.py"]
    os.makedirs(wd + "/ecf", exist_ok=True)
    for rev in revs:
        for f in files:
            lfile = wd + "/" + f
            rfile = rev + "/" + f
            if not os.path.exists(lfile):
                if debug:
                    print(rfile, lfile)
                if os.path.exists(rfile) and lfile != rfile:
                    if debug:
                        print("Copy " + rfile + " -> " + lfile)
                    shutil.copy2(rfile, lfile)
                    fetched.append(lfile)
            else:
                if lfile not in fetched:
                    print(lfile + " already exists, is not fetched again from " + rev)

    print("Copy namelists from ", revs)
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
                        if debug:
                            print("Copy " + rfile + " -> " + lfile)
                        # shutil.copytree(rdir, ldir)
                        shutil.copy2(rfile, lfile)
                        fetched.append(lfile)
                else:
                    if lfile not in fetched:
                        print(lfile + " already exists, is not fetched again from " + rev)

    # Set up offline
    revs = [revision]
    config_dirs = ["offline"]
    for rev in revs:
        for cdir in config_dirs:
            if debug:
                print(rev + "/" + cdir, wd + "/" + cdir)
            if os.path.exists(rev + "/" + cdir):
                if not os.path.exists(wd + "/" + cdir):
                    print("Copy source code " + rev + "/" + cdir + " -> " + wd + "/" + cdir)
                    shutil.copytree(rev + "/" + cdir, wd + "/" + cdir)
                else:
                    print(wd + "/" + cdir + " already exists, is not fetched again from " + rev)

    json.dump(paths_to_sync, open(wd + "/paths_to_sync.json", "w"), indent=2)


def parse_surfex_script_setup(argv):
    """Parse the command line input arguments."""
    parser = ArgumentParser("Surfex offline setup script")
    parser.add_argument('-exp_name', dest="exp", help="Experiment name", type=str, default=None)
    parser.add_argument('--wd', help="Experiment working directory", type=str, default=None)

    parser.add_argument('-dtg', help="DateTimeGroup (YYYYMMDDHH)", type=str, required=False, default=None)
    parser.add_argument('-dtgend', help="DateTimeGroup (YYYYMMDDHH)", type=str, required=False, default=None)
    parser.add_argument('--suite', type=str, default="surfex", required=False, help="Type of suite definition")
    parser.add_argument('--stream', type=str, default=None, required=False, help="Stream")

    # Setup variables
    parser.add_argument('-rev', dest="rev", help="Surfex experiement source revison", type=str, required=False,
                        default=None)
    parser.add_argument('-surfex', dest="pysurfex", help="Pysurfex library", type=str, required=False,
                        default=None)
    parser.add_argument('-experiment', dest="pysurfex_experiment", help="Pysurfex-experiment library", type=str,
                        required=False, default=None)
    parser.add_argument('-offline', dest="offline_source", help="Offline source code", type=str,
                        required=False, default=None)
    parser.add_argument('-host', dest="host", help="Host label for setup files", type=str, required=False,
                        default=None)
    parser.add_argument('--config', help="Config", type=str, required=False, default=None)
    parser.add_argument('--config_file', help="Config file", type=str, required=False, default=None)
    parser.add_argument('--debug', dest="debug", action="store_true", help="Debug information")
    parser.add_argument('--version', action='version', version=__version__)

    print(" ".join(argv))
    if len(argv) == 0:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args(argv)
    kwargs = {}
    for arg in vars(args):
        kwargs.update({arg: getattr(args, arg)})
    return kwargs


def surfex_script_setup(**kwargs):

    debug = False
    if "debug" in kwargs:
        debug = kwargs["debug"]

    exp_name = None
    if "exp" in kwargs:
        exp_name = kwargs["exp"]

    # Setup
    wd = None
    if "wd" in kwargs:
        wd = kwargs["wd"]
    rev = None
    if "rev" in kwargs:
        rev = kwargs["rev"]
    pysurfex = None
    if "pysurfex" in kwargs:
        pysurfex = kwargs["pysurfex"]
    pysurfex_experiment = None
    if "pysurfex_experiment" in kwargs:
        pysurfex_experiment = kwargs["pysurfex_experiment"]
    offline_source = None
    if "offline_source" in kwargs:
        offline_source = kwargs["offline_source"]
    host = kwargs["host"]
    config = None
    if "config" in kwargs:
        config = kwargs["config"]
    config_file = None
    if "config_file" in kwargs:
        config_file = kwargs["config_file"]

    # Find experiment
    if wd is None:
        wd = os.getcwd()
        print("Setting current working directory as WD: " + wd)
    if exp_name is None:
        print("Setting EXP from WD:" + wd)
        exp_name = wd.split("/")[-1]
        print("EXP = " + exp_name)

    # Copy files to WD from REV
    if rev is None:
        if pysurfex_experiment is None:
            raise Exception("You must set REV or pysurfex_experiment")
        else:
            print("Using " + pysurfex_experiment + " as rev")
            rev = pysurfex_experiment
    if host is None:
        raise Exception("You must set host")
    if pysurfex is None:
        if rev is not None:
            print("Using " + rev + " as pysurfex")
            pysurfex = rev
        else:
            raise Exception("pysurfex must be set when rev is not set")
    if pysurfex_experiment is None:
        if rev is not None:
            print("Using " + rev + " as pysurfex_experiment")
            pysurfex_experiment = rev
        else:
            raise Exception("pysurfex_experiment must be set when rev is not set")
    if offline_source is None:
        print("No offline soure code set. Assume existing binaries")

    setup_files(wd, exp_name, host,  rev, pysurfex_experiment, pysurfex,
                offline_source=offline_source,
                configuration=config, configuration_file=config_file, debug=debug)
