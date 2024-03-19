"""Experiment tools."""
import collections
import json
import yaml
import os
import shutil

import pysurfex
import tomlkit

import experiment
from deode.datetime_utils import as_datetime, as_timedelta
from deode.logs import logger


def get_nnco(config, basetime=None, realization=None):
    """Get the active observations.

    Args:
        basetime (as_datetime, optional): Basetime. Defaults to None.
        realization (int, optional): Realization number

    Returns:
        list: List with either 0 or 1

    """
    # Some relevant assimilation settings
    obs_types = get_setting(config, "SURFEX.ASSIM.OBS.COBS_M", realization=realization)
    nnco_r = get_setting(config, "SURFEX.ASSIM.OBS.NNCO", realization=realization)
    snow_ass = get_setting(
        config, "SURFEX.ASSIM.ISBA.UPDATE_SNOW_CYCLES", realization=realization
    )
    snow_ass_done = False

    if basetime is None:
        basetime = as_datetime(config["general.times.basetime"])
    if len(snow_ass) > 0:
        if basetime is not None:
            hhh = int(basetime.strftime("%H"))
            for s_n in snow_ass:
                if hhh == int(s_n):
                    snow_ass_done = True
    nnco = []
    for ivar, __ in enumerate(obs_types):
        ival = 0
        if nnco_r[ivar] == 1:
            ival = 1
            if obs_types[ivar] == "SWE":
                if not snow_ass_done:
                    logger.info(
                        "Disabling snow assimilation since cycle is not in {}",
                        snow_ass,
                    )
                    ival = 0
        logger.debug("ivar={} ival={}", ivar, ival)
        nnco.append(ival)

    logger.debug("NNCO: {}", nnco)
    return nnco


def get_total_unique_cycle_list(config):
    """Get a list of unique start times for the forecasts.

    Returns:
        list: List with time deltas from midnight
    """
    # Create a list of all cycles from all members
    realizations = config["general.realizations"]
    if realizations is None or len(realizations) == 0:
        return get_cycle_list(config)
    else:
        cycle_list_all = []
        for realization in realizations:
            cycle_list_all += get_cycle_list(config, realization=realization)

        cycle_list = []
        cycle_list_str = []
        for cycle in cycle_list:
            cycle_str = str(cycle)
            if cycle_str not in cycle_list_str:
                cycle_list.append(cycle)
                cycle_list_str.append(str(cycle))
        return cycle_list


def get_cycle_list(config, realization=None):
    """Get cycle list as time deltas from midnight.

    Args:
        realization (int, optional): Realization number

    Returns:
        list: Cycle list
    """
    cycle_length = as_timedelta(
        get_setting(config, "general.times.cycle_length", realization=realization)
    )
    cycle_list = []
    day = as_timedelta("PT24H")

    cycle_time = cycle_length
    while cycle_time <= day:
        cycle_list.append(cycle_time)
        cycle_time += cycle_length
    return cycle_list


def get_setting(config, setting, sep="#", realization=None):
    """Get setting.

    Args:
        setting (str): Setting
        sep (str, optional): _description_. Defaults to "#".
        realization (int, optional): Realization number

    Returns:
        any: Found setting

    """
    items = setting.replace(sep, ".")
    logger.info("Could check realization {}", realization)
    return config[items]


def setting_is(config, setting, value, realization=None):
    """Check if setting is value.

    Args:
        setting (str): Setting
        value (any): Value
        realization (int, optional): Realization number

    Returns:
        bool: True if found, False if not found.
    """
    if get_setting(config, setting, realization=realization) == value:
        return True
    return False


def get_fgint(config, realization=None):
    """Get the fgint.

    Args:
        realization (int, optional): Realization number

    Returns:
        as_timedelta: fgint

    """
    return as_timedelta(
        get_setting(config, "general.times.cycle_length", realization=realization)
    )


class Exp:
    """Experiment class. Copy of deode class, to be replaced"""

    def __init__(
        self,
        config,
        exp_dependencies,
        merged_config,
        platform_paths_file,
        scheduler_config_file,
        submission_config_file,
        domain_file,
    ):
        """Instanciate an object of the main experiment class.

        Args:
            config (.config_parser.ParsedConfig): Parsed config file contents.
            exp_dependencies (dict):  Eperiment dependencies
            merged_config (dict): Experiment configuration
            platform_paths_file (dict): Platform path settings
            scheduler_config_file (str): Scheduler config file
            submission_config_file (str): Submission config file
            domain_file (str): Domain file

        """
        logger.debug("Construct Exp")

        troika_settings = {}
        try:
            troika_config = merged_config["troika"]["config_file"]
            troika_settings.update({"config_file": troika_config})
        except KeyError:
            logger.debug(
                "Troika config not found in merged_config. Should be in default config."
            )

        try:
            troika = merged_config["troika"]["troika"]
            troika_settings.update({"troika": troika})
        except KeyError:
            try:
                troika = shutil.which("troika")
                troika_settings.update({"troika": troika})
            except RuntimeError:
                logger.warning("Troika not found!")

        config_toml = config.metadata["source_file_path"]
        with open(config_toml, mode="r", encoding="utf8") as fh:
            default_config = tomlkit.load(fh)

        merged_config = ExpFromFiles.deep_update(default_config, merged_config)
        if scheduler_config_file is None:
            scheduler_config_file = merged_config["include"]["scheduler"]
        if submission_config_file is None:
            submission_config_file = merged_config["include"]["submission"]
        if domain_file is None:
            domain_file = merged_config["include"]["domain"]
        if platform_paths_file is None:
            platform_paths_file = merged_config["include"]["platform"]
        update = {
            "include": {
                "scheduler": f"{scheduler_config_file}",
                "submission": f"{submission_config_file}",
                "domain": f"{domain_file}",
                "platform": f"{platform_paths_file}",
            },
            "troika": troika_settings,
        }

        merged_config = ExpFromFiles.deep_update(merged_config, update)
        config_dir = exp_dependencies.get("config_dir")

        for key, val in merged_config["include"].items():
            lval = val.replace("@config_dir@", config_dir)
            merged_config["include"][key] = lval
        self.config = merged_config

    def save_as(self, fname):
        """Save case config file.

        Args:
            fname (str): File name.
        """
        with open(fname, mode="w", encoding="utf8") as fh:
            tomlkit.dump(self.config, fh)


class Exp2(Exp):
    """Experiment class."""

    def __init__(
        self,
        config,
        exp_dependencies,
        merged_config,
        platform_paths_file,
        scheduler_config_file,
        submission_config_file,
        domain_file,
    ):
        """Instanciate an object of the main experiment class.

        Args:
            config (.config_parser.ParsedConfig): Parsed config file contents.
            exp_dependencies (dict):  Eperiment dependencies
            merged_config (dict): Experiment configuration
            platform_paths_file (dict): Platform path settings
            scheduler_config_file (str): Scheduler config file
            submission_config_file (str): Submission config file
            domain_file (str): Domain file

        """
        logger.debug("Construct Exp")

        troika_config = merged_config["troika"]["config_file"]
        default_config_dir = exp_dependencies.get("default_config_dir")
        troika_config = troika_config.replace("@default_config_dir@", default_config_dir)
        try:
            troika = merged_config["troika"]["troika"]
        except KeyError:
            try:
                troika = shutil.which("troika")
            except RuntimeError:
                logger.warning("Troika not found!")
                troika = None

        config_toml = config.metadata["source_file_path"]
        with open(config_toml, mode="r", encoding="utf8") as fh:
            default_config = tomlkit.load(fh)

        merged_config = ExpFromFiles.deep_update(default_config, merged_config)
        stream = exp_dependencies.get("stream")
        if stream is None:
            stream = ""

        update = {
            "general": {
                "stream": stream,
            },
            "system": {
                "climdir": "@casedir@/climate/",
                "archive_dir": "@casedir@/archive/@YYYY@/@MM@/@DD@/@HH@/",
                "extrarch_dir": "@casedir@/archive/extract/",
                "forcing_dir": "@casedir@/forcing/@YYYY@@MM@@DD@@HH@/@RRR@/",
                "obs_dir": "@casedir@/archive/observations/@YYYY@/@MM@/@DD@/@HH@/",
                "namelist_defs": f"{exp_dependencies.get('namelist_defs')}",
                "binary_input_files": f"{exp_dependencies.get('binary_input_files')}",
                "first_guess_yml": f"{merged_config['pysurfex']['first_guess_yml_file']}",
                "config_yml": f"{merged_config['pysurfex']['forcing_variable_config_yml_file']}",
            },
            "include": {
                "scheduler": f"{scheduler_config_file}",
                "submission": f"{submission_config_file}",
                "domain": f"{domain_file}",
                "platform": f"{platform_paths_file}",
            },
            "troika": {"troika": troika, "config_file": troika_config},
            "SURFEX": merged_config["SURFEX"],
        }

        merged_config = ExpFromFiles.deep_update(merged_config, update)
        config_dir = exp_dependencies.get("config_dir")

        for key, val in merged_config["include"].items():
            val = val.replace("@config_dir@", config_dir)
            merged_config["include"][key] = val
        self.config = merged_config

        Exp.__init__(
            self,
            config,
            exp_dependencies,
            merged_config,
            platform_paths_file,
            scheduler_config_file,
            submission_config_file,
            domain_file
        )

    def save_as(self, fname):
        """Save case config file.

        Args:
            fname (str): File name.
        """
        with open(fname, mode="w", encoding="utf8") as fh:
            tomlkit.dump(self.config, fh)


class ExpFromFiles(Exp2):
    """Generate Exp object from existing files. Use config files from a setup."""

    def __init__(
        self,
        config,
        exp_dependencies,
        stream=None,
        config_settings=None,
    ):
        """Construct an Exp object from files.

        Args:
            exp_dependencies (dict): Exp dependencies
            stream(str, optional): Stream identifier
            config_settings(dict): Possible input config settings

        Raises:
            FileNotFoundError: If host file(s) not found
            KeyError: Key not found

        """
        logger.debug("Construct ExpFromFiles")
        logger.debug("Experiment dependencies: {}", exp_dependencies)

        # platform paths
        try:
            platform_paths_file = exp_dependencies["include_paths"]["platform"]
        except KeyError:
            platform_paths_file = None

        # Submission settings
        try:
            submission_config_file = exp_dependencies["include_paths"]["submission"]
        except KeyError:
            submission_config_file = None

        # Scheduler settings
        try:
            scheduler_config_file = exp_dependencies["include_paths"]["scheduler"]
        except KeyError:
            scheduler_config_file = None

        config_settings = {}
        case = exp_dependencies.get("case")
        if case is not None:
            if "general" not in config_settings:
                config_settings.update({"general": {}})
            config_settings["general"].update({"case": case})

        experiment_path = f"{experiment.__path__[0]}/.."
        plugin_registry = {"plugins": {"experiment": experiment_path}}

        plugin_registry_file = f"{exp_dependencies.get('default_config_dir')}/deode_plugins.yml"
        with open(plugin_registry_file, mode="w", encoding="utf8") as fh:
            yaml.safe_dump(plugin_registry, fh)
        config_settings = {"general": {"case": case, "plugin_registry": plugin_registry_file}}
        pysurfex_exp_defaults = self.toml_load(
            exp_dependencies.get("pysurfex_experiment_default_config")
        )
        config_settings = self.deep_update(config_settings, pysurfex_exp_defaults)

        pysurfex_config_file = config_settings["pysurfex"]["config_file"]
        pysurfex_config_file = pysurfex_config_file.replace(
            "@pysurfex_cfg@", f"{pysurfex.__path__[0]}/cfg"
        )
        pysurfex_config = self.toml_load(pysurfex_config_file)
        config_settings = self.deep_update(config_settings, pysurfex_config)

        base_config = exp_dependencies.get("base_config_file")
        if base_config is not None:
            base_config = self.toml_load(base_config)
            config_settings = self.deep_update(config_settings, base_config)

        case_config = exp_dependencies.get("case_config_file")
        if case_config is not None:
            case_config = self.toml_load(case_config)
            config_settings = self.deep_update(config_settings, case_config)

        for key, val in config_settings["pysurfex"].items():
            val = val.replace("@pysurfex_cfg@", f"{pysurfex.__path__[0]}/cfg")
            config_settings["pysurfex"][key] = val

        ial_source = exp_dependencies.get("ial_source")
        if ial_source is not None:
            if "compile" not in config_settings:
                config_settings.update({"compile": {}})
            config_settings["compile"].update({"ial_source": ial_source})

        try:
            domain_file = exp_dependencies["include_paths"]["domain"]
        except KeyError:
            domain_file = None

        Exp2.__init__(
            self,
            config,
            exp_dependencies,
            config_settings,
            platform_paths_file,
            scheduler_config_file,
            submission_config_file,
            domain_file,
        )

    @staticmethod
    def toml_load(fname):
        """Load from toml file.

        Using tomlkit to preserve stucture

        Args:
            fname (str): Filename

        Returns:
            _type_: _description_

        """
        with open(fname, "r", encoding="utf-8") as f_h:
            res = tomlkit.parse(f_h.read())
        return res

    @staticmethod
    def toml_dump(to_dump, fname):
        """Dump toml to file.

        Using tomlkit to preserve stucture

        Args:
            to_dump (_type_): _description_
            fname (str): Filename

        """
        with open(fname, mode="w", encoding="utf-8") as f_h:
            f_h.write(tomlkit.dumps(to_dump))

    @staticmethod
    def deep_update(source, overrides):
        """Update a nested dictionary or similar mapping.

        Modify ``source`` in place.

        Args:
            source (dict): Source
            overrides (dict): Updates

        Returns:
            dict: Updated dictionary

        """
        for key, value in overrides.items():
            if isinstance(value, collections.abc.Mapping) and value:
                returned = ExpFromFiles.deep_update(source.get(key, {}), value)
                source[key] = returned
            else:
                override = overrides[key]

                source[key] = override

        return source

    @staticmethod
    def setup_files(
        case,
        domain,
        host,
        base_config_file=None,
        case_config_file=None,
        config_dir=None,
        ial_source=None,
        submission_file=None,
        namelist_defs=None,
        binary_input_files=None,
    ):
        """Set up the files for an experiment.

        Args:
            case (str): Experiment name
            domain (str): Domain name
            host (str): Host label
            base_config_file (str, optional): Base config file. Defaults to None.
            case_config_file (str, optional): Case specific config file. Defaults to None.
            config_dir (str, optional): Config directory. Defaults to None.
            ial_source (str, optional): IAL source code. Defaults to None.
            submission_file (str, optional): Submission file. Defaults to None.
            namelist_defs (str, optional): Namelist directory. Defaults to None.
            binary_input_files (str, optional): Binary input files. Defaults to None.

        Raises:
            FileNotFoundError: System files not found

        Returns:
            exp_dependencies(dict): Experiment dependencies from setup.

        """
        exp_dependencies = {}
        include_paths = {}

        if config_dir is None:
            config_dir = f"{os.getcwd()}/data/config"
            logger.info(
                "Setting config_dir from current working directory: {}", config_dir
            )

        # Set submission file if provided
        if submission_file is not None:
            include_paths.update({"submission": submission_file})
        if host is not None:
            logger.info("Setting up for host {}", host)
            include_paths.update(
                {
                    "scheduler": f"{config_dir}/include/scheduler/ecflow_{host}.toml",
                    "platform": f"{config_dir}/include/platform_paths/{host}.toml",
                }
            )
            if domain is not None:
                include_paths.update(
                    {"domain": f"{config_dir}/include/domains/{domain}.toml"}
                )
            if "submission" not in include_paths:
                include_paths.update(
                    {"submission": f"{config_dir}/include/submission/{host}.toml"}
                )
            for incp in include_paths.values():
                if not os.path.exists(incp):
                    logger.error("Input file {} not found", incp)
                    raise FileNotFoundError(incp)

        # Check existence of needed config files
        config = None
        default_config_dir = f"{config_dir}/defaults/"
        config_file = f"{config_dir}/defaults/config.toml"
        if os.path.exists(config_file):
            logger.info("Config definition {}", config_file)
            config = ExpFromFiles.toml_load(config_file)
        else:
            raise FileNotFoundError("Config file not found")

        # default_config_dir
        pysurfex_experiment_default_config_file = config[
            "pysurfex_experiment_default_config"
        ]
        pysurfex_experiment_default_config_file = (
            pysurfex_experiment_default_config_file.replace(
                "@default_config_dir@", f"{config_dir}/defaults/"
            )
        )

        if namelist_defs is None:
            namelist_defs = f"{config_dir}/nam/surfex_namelists.yml"
            logger.info("Using default namelist directory {}", namelist_defs)

        if binary_input_files is None:
            binary_input_files = f"{config_dir}/input/binary_input_data.json"
            logger.info("Using default binary input {}", binary_input_files)

        exp_dependencies.update(
            {
                "config_dir": config_dir,
                "default_config_dir": default_config_dir,
                "pysurfex_experiment_default_config": pysurfex_experiment_default_config_file,
                "base_config_file": base_config_file,
                "case_config_file": case_config_file,
                "include_paths": include_paths,
                "case": case,
                "ial_source": ial_source,
                "namelist_defs": namelist_defs,
                "binary_input_files": binary_input_files,
            }
        )
        return exp_dependencies

    @staticmethod
    def dump_exp_dependencies(exp_dependencies, exp_dependencies_file, indent=2):
        """Dump an experiment dependency file.

        Args:
            exp_dependencies (dict): Experiment dependencies
            exp_dependencies_file (str): Filename to dump to
            indent (int, optional): Intendation. Defaults to 2.
        """
        with open(exp_dependencies_file, mode="w", encoding="utf-8") as fh:
            json.dump(exp_dependencies, fh, indent=indent)


class ExpFromFilesDep(ExpFromFiles):
    """Generate Exp object from existing files. Use config files from a setup."""

    def __init__(
        self,
        config,
        exp_dependencies,
        stream=None,
        config_settings=None,
    ):
        """Construct an Exp object from files.

        Args:
            config (.config_parser.ParsedConfig): Parsed config file contents.
            exp_dependencies (str): File with exp dependencies
            stream (str): Stream identifier

        """
        logger.debug("Construct ExpFromFilesDep")
        ExpFromFiles.__init__(
            self,
            config,
            exp_dependencies,
            stream=stream,
            config_settings=config_settings,
        )


class ExpFromFilesDepFile(ExpFromFiles):
    """Generate Exp object from existing files. Use config files from a setup."""

    def __init__(
        self,
        config,
        exp_dependencies_file,
        stream=None,
    ):
        """Construct an Exp object from files.

        Args:
            config (.config_parser.ParsedConfig): Parsed config file contents.
            exp_dependencies_file (str): File with exp dependencies
            stream (str): Stream identifier

        Raises:
            FileNotFoundError: If file is not found

        """
        logger.debug("Construct ExpFromFilesDepFile")
        if os.path.exists(exp_dependencies_file):
            with open(
                exp_dependencies_file, mode="r", encoding="utf-8"
            ) as exp_dependencies_file:
                exp_dependencies = json.load(exp_dependencies_file)
                ExpFromFiles.__init__(
                    self,
                    config,
                    exp_dependencies,
                    stream=stream,
                )
        else:
            raise FileNotFoundError(
                f"Experiment dependencies not found {exp_dependencies_file}"
            )


def case_setup(
    config,
    output_file,
    case=None,
    domain=None,
    host=None,
    config_dir=None,
    submission_file=None,
    ial_source=None,
    namelist_defs=None,
    binary_input_files=None,
    base_config_file=None,
    case_config_file=None,
):
    """Do experiment setup.

    Args:
        config (.config_parser.ParsedConfig): Parsed config file contents.
        output_file (str): Output config file.
        case (str, optional): Case identifier. Defaults to None.
        domain (str, optional): domain name. Defaults to None.
        host (str, optional): host name. Defaults to None.
        config_dir (str, optional): Configuration directory. Defaults to None.
        submission_file (str, optional): Submission file. Defaults to None.
        ial_source (str, optional): IAL source path. Defaults to None.
        base_config_file (str, optional): Base configuration file. Defaults to None.
        case_config_file (str, optional): Case specific configuration file.
                                          Defaults to None.

    """
    logger.info("************ CaseSetup ******************")

    exp_dependencies_file = f"{os.getcwd()}/.exp_dependencies_{case}.json"
    if ial_source is None:
        logger.warning("No soure code set. Assume existing binaries")

    exp_dependencies = ExpFromFiles.setup_files(
        case,
        domain,
        host,
        base_config_file=base_config_file,
        case_config_file=case_config_file,
        config_dir=config_dir,
        submission_file=submission_file,
        ial_source=ial_source,
        namelist_defs=namelist_defs,
        binary_input_files=binary_input_files,
    )

    logger.debug("Store exp dependencies in {}", exp_dependencies_file)
    ExpFromFiles.dump_exp_dependencies(exp_dependencies, exp_dependencies_file)

    sfx_exp = ExpFromFilesDepFile(config, exp_dependencies_file)
    sfx_exp.save_as(output_file)
