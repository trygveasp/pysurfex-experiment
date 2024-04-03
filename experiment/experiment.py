"""Experiment tools."""
import os
import shutil

import pysurfex
from deode.datetime_utils import as_datetime, as_timedelta
from deode.experiment import ExpFromFiles
from deode.host_actions import DeodeHost
from deode.logs import logger

import experiment


def get_nnco(config, basetime=None, realization=None):
    """Get the active observations.

    Args:
        config (.config_parser.ParsedConfig): Parsed config file contents.
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
    if len(snow_ass) > 0 and basetime is not None:
        hhh = int(basetime.strftime("%H"))
        for s_n in snow_ass:
            if hhh == int(s_n):
                snow_ass_done = True
    nnco = []
    for ivar, __ in enumerate(obs_types):
        ival = 0
        if nnco_r[ivar] == 1:
            ival = 1
            if obs_types[ivar] == "SWE" and not snow_ass_done:
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

    Args:
        config (.config_parser.ParsedConfig): Parsed config file contents.

    Returns:
        list: List with time deltas from midnight

    """
    # Create a list of all cycles from all members
    realizations = config["general.realizations"]
    if realizations is None or len(realizations) == 0:
        return get_cycle_list(config)

    cycle_list_all = []
    for realization in realizations:
        cycle_list_all += get_cycle_list(config, realization=realization)

    cycle_list = []
    cycle_list_str = []
    for cycle in cycle_list_all:
        cycle_str = str(cycle)
        if cycle_str not in cycle_list_str:
            cycle_list.append(cycle)
            cycle_list_str.append(str(cycle))
    return cycle_list


def get_cycle_list(config, realization=None):
    """Get cycle list as time deltas from midnight.

    Args:
        config (.config_parser.ParsedConfig): Parsed config file contents.
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
        config (.config_parser.ParsedConfig): Parsed config file contents.
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
        config (.config_parser.ParsedConfig): Parsed config file contents.
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
        config (.config_parser.ParsedConfig): Parsed config file contents.
        realization (int, optional): Realization number

    Returns:
        as_timedelta: fgint

    """
    return as_timedelta(
        get_setting(config, "general.times.cycle_length", realization=realization)
    )


'''
class Exp:
    """Experiment class. Copy of deode class, to be replaced."""

    def __init__(
        self,
        config,
        merged_config,
    ):
        """Instanciate an object of the main experiment class.

        Args:
            config (.config_parser.ParsedConfig): Parsed config file contents.
            merged_config (dict): Experiment configuration

        """
        logger.debug("Construct Exp")
        config = config.copy(update=merged_config)
        self.config = config


class ExpFromFiles(Exp):
    """Generate Exp object from existing files. Use config files from a setup."""

    def __init__(
        self,
        config,
        exp_dependencies,
        mod_files,
        host=None,
        merged_config=None
    ):
        """Construct an Exp object from files.

        Args:
            config (.config_parser.ParsedConfig): Parsed config file contents.
            exp_dependencies (dict): Exp dependencies
            mod_files (list): Case modifications

        Raises:
            FileNotFoundError: If host file(s) not found

        """
        logger.debug("Construct ExpFromFiles")
        logger.debug("Experiment dependencies: {}", exp_dependencies)

        config_dir = exp_dependencies.get("config_dir")
        include_paths = {}
        if host is not None:
            host = host.detect_deode_host()
            logger.info("Setting up for host {}", host)
            include_paths.update(
                {
                    "scheduler": f"{config_dir}/include/scheduler/ecflow_{host}.toml",
                    "platform": f"{config_dir}/include/platform_paths/{host}.toml",
                    "submission": f"{config_dir}/include/submission/{host}.toml"
                }
            )

        domain = exp_dependencies.get("domain_name")
        if domain is not None:
            include_paths.update(
                {
                    "domain": f"{config_dir}/include/domains/{domain}.toml",
                })

        for inct, incp in include_paths.items():
            if not os.path.exists(incp):
                logger.error("Input file {} not found", incp)
                raise FileNotFoundError(incp)

        # Since include is parsed in at the beginning and we do not want
        # default platform/host/submission/domain values not being overridden
        # to stay in the config, we make a temporary copy which we modify and
        # re-read the config
        config_toml = config.metadata["source_file_path"]
        tmp_outfile = exp_dependencies.get("tmp_outfile")
        shutil.copy(config_toml, tmp_outfile)
        with open(tmp_outfile, mode="r", encoding="utf8") as fh:
            mod_config = tomlkit.load(fh)

        for inct, incp in include_paths.items():
            mod_config["include"].update({inct: incp})
        with open(tmp_outfile, mode="w", encoding="utf8") as fh:
            tomlkit.dump(mod_config, fh)
        config = ParsedConfig.from_file(tmp_outfile, json_schema=ConfigParserDefaults.MAIN_CONFIG_JSON_SCHEMA)
        os.remove(tmp_outfile)

        mods = {}
        for mod in mod_files:
            lmod = ExpFromFiles.toml_load(mod)
            logger.info("Merging modifications from {}: {}", mod, lmod)
            mods = ExpFromFiles.deep_update(mods, lmod)

        case = exp_dependencies.get("case")
        if case is not None:
            if "general" not in mods:
                mods.update({"general": {}})
            mods["general"].update({"case": case})

        # Merge with possible incoming modifications
        if merged_config is None:
            merged_config = {}
        merged_config = ExpFromFiles.deep_update(merged_config, mods)
        Exp.__init__(
            self,
            config,
            merged_config,
        )

    @staticmethod
    def toml_load(fname):
        """Load from toml file.

        Using tomlkit to preserve stucture

        Args:
            fname (str): Filename

        Returns:
            dict: Loaded toml file

        """
        with open(fname, "r", encoding="utf8") as f_h:
            res = tomlkit.parse(f_h.read())
        return res

    @staticmethod
    def toml_dump(to_dump, fname):
        """Dump toml to file.

        Using tomlkit to preserve stucture

        Args:
            to_dump (dict): Data to save
            fname (str): Filename

        """
        with open(fname, mode="w", encoding="utf8") as f_h:
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
        output_file,
        case=None,
        domain=None,
        config_dir=None,
    ):
        """Set up the files for an experiment.

        Args:
            output_file (str): Output file
            case (str, optional): Experiment name. Defaults to None.
            domain (str, optional): Domain name. Defaults to None.
            config_dir (str, optional): Config directory. Defaults to None.

        Returns:
            exp_dependencies(dict): Experiment dependencies from setup.

        """
        exp_dependencies = {}
        if config_dir is None:
            config_dir = f"{os.getcwd()}/data/config"
            logger.info(
                "Setting config_dir from current working directory: {}", config_dir
            )

        exp_dependencies.update(
            {
                "tmp_outfile": f"{output_file}.tmp.{os.getpid()}.toml",
                "config_dir": config_dir,
                "case": case,
                "domain_name": domain,
            }
        )
        return exp_dependencies
'''


class PyExpFromFiles(ExpFromFiles):
    """Generate Exp object from existing files. Use config files from a setup."""

    def __init__(
        self,
        config,
        exp_dependencies,
        mod_files,
        host=None,
    ):
        """Construct an Exp object from files.

        Args:
            config (.config_parser.ParsedConfig): Parsed config file contents.
            exp_dependencies (dict): Exp dependencies
            mod_files (list): Case modifications

        Raises:
            FileNotFoundError: If host file(s) not found
            KeyError: Key not found

        """
        logger.debug("Construct ExpFromFiles")
        logger.debug("Experiment dependencies: {}", exp_dependencies)

        config_dir = exp_dependencies.get("config_dir")
        config_settings = {}

        experiment_path = f"{experiment.__path__[0]}/.."
        plugin_registry = {"plugins": {"experiment": experiment_path}}

        if "general" not in config_settings:
            config_settings.update({"general": {}})
        config_settings["general"].update({"plugin_registry": plugin_registry})
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

        for key, val in config_settings["pysurfex"].items():
            lval = val.replace("@pysurfex_cfg@", f"{pysurfex.__path__[0]}/cfg")
            config_settings["pysurfex"][key] = lval

        troika_config = config_settings["troika"]["config_file"]
        default_config_dir = exp_dependencies.get("default_config_dir")
        troika_config = troika_config.replace("@default_config_dir@", default_config_dir)
        try:
            troika = config_settings["troika"]["troika"]
        except KeyError:
            try:
                troika = shutil.which("troika")
            except RuntimeError:
                logger.warning("Troika not found!")
                troika = None

        try:
            bindir = config_settings["system"]["bindir"]
        except KeyError:
            bindir = "@casedir@/offline/exe"

        config_dir = exp_dependencies["config_dir"]
        namelist_defs = exp_dependencies.get("namelist_defs")
        if namelist_defs is None:
            try:
                namelist_defs = config_settings["system"]["namelist_defs"]
            except KeyError:
                namelist_defs = f"{config_dir}/nam/surfex_namelists.yml"
                logger.info("Using default namelist directory {}", namelist_defs)

        binary_input_files = exp_dependencies.get("binary_input_files")
        if binary_input_files is None:
            try:
                binary_input_files = config_settings["system"]["binary_input_files"]
            except KeyError:
                binary_input_files = f"{config_dir}/input/binary_input_data.json"
                logger.info("Using default binary input {}", binary_input_files)

        config_yml = f"{config_settings['pysurfex']['forcing_variable_config_yml_file']}"
        update = {
            "system": {
                "namelist_defs": f"{namelist_defs}",
                "binary_input_files": f"{binary_input_files}",
                "first_guess_yml": f"{config_settings['pysurfex']['first_guess_yml_file']}",
                "config_yml": config_yml,
                "bindir": bindir,
            },
            "troika": {"troika": troika, "config_file": troika_config},
            "SURFEX": config_settings["SURFEX"],
        }

        merged_config = ExpFromFiles.deep_update(config_settings, update)
        ExpFromFiles.__init__(
            self,
            config,
            exp_dependencies,
            mod_files,
            host=host,
            merged_config=merged_config,
        )


def case_setup(
    config,
    output_file,
    mod_files,
    case=None,
    host=None,
    config_dir=None,
    domain=None,
):
    """Do experiment setup.

    Args:
        config (.config_parser.ParsedConfig): Parsed config file contents.
        output_file (str): Output config file.
        mod_files (list): Modifications. Defaults to None.
        case (str, optional): Case identifier. Defaults to None.
        host (str, optional): host name. Defaults to None.
        config_dir (str, optional): Configuration directory. Defaults to None.
        domain (str, optional): Domain name. Defaults to None.

    """
    logger.info("************ CaseSetup ******************")

    exp_dependencies = ExpFromFiles.setup_files(
        output_file,
        case=case,
        domain=domain,
        config_dir=config_dir,
    )

    rootdir = f"{os.path.dirname(__file__)}"
    default_config_dir = f"{rootdir}/../data/config/defaults"
    py_exp_def_conf_file = f"{default_config_dir}/pysurfex_experiment_defaults.toml"
    exp_dependencies.update(
        {
            "default_config_dir": default_config_dir,
            "pysurfex_experiment_default_config": py_exp_def_conf_file,
        }
    )

    sfx_exp = PyExpFromFiles(config, exp_dependencies, mod_files, host=host)
    sfx_exp.config.save_as(output_file)
