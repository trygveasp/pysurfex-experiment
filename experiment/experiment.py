"""Experiment classes and methods."""
import os
import json
import logging
import scheduler
import toml
import experiment


class Exp(experiment.Configuration):
    """Experiment class."""

    def __init__(self, exp_dependencies, merged_config):
        """Instaniate an object of the main experiment class.

        Args:
            exp_dependencies (dict):  Eperiment dependencies
            merged_config (dict): Experiment configuration

        """
        logging.debug("Construct Exp")
        offline_source = exp_dependencies.get("offline_source")
        wdir = exp_dependencies.get("exp_dir")
        exp_name = exp_dependencies.get("exp_name")

        self.name = exp_name
        merged_config["GENERAL"].update({"EXP": exp_name})
        merged_config["GENERAL"].update({"EXP_DIR": wdir})
        self.work_dir = wdir
        self.scripts = exp_dependencies.get("pysurfex_experiment")
        self.pysurfex = exp_dependencies.get("pysurfex")
        self.offline_source = offline_source
        self.config_file = None

        experiment.Configuration.__init__(self, merged_config)

    def dump_exp_configuration(self, filename, indent=None):
        """Dump the exp configuration.

        The configuration has two keys. One for general config and one for member config.

        Args:
            filename (str): filename to dump to
            indent (int, optional): indentation in json file. Defaults to None.
        """
        json.dump(self.settings, open(filename, mode="w", encoding="utf-8"), indent=indent)
        self.config_file = filename


class ExpFromFiles(Exp):
    """Generate Exp object from existing files. Use config files from a setup."""

    def __init__(self, exp_dependencies_file, stream=None):
        """Construct an Exp object from files.

        Args:
            exp_dependencies_file (str): File with exp dependencies

        Raises:
            FileNotFoundError: If file is not found

        """
        logging.debug("Construct ExpFromFiles")
        if os.path.exists(exp_dependencies_file):
            with open(exp_dependencies_file, mode="r", encoding="utf-8") as exp_dependencies_file:
                exp_dependencies = json.load(exp_dependencies_file)
        else:
            raise FileNotFoundError("Experiment dependencies not found " + exp_dependencies_file)

        wdir = exp_dependencies.get("exp_dir")
        self.work_dir = wdir

        # System
        env_system = wdir + "/Env_system"
        if os.path.exists(env_system):
            with open(env_system, mode="r", encoding="utf-8") as env_system:
                env_system = toml.load(env_system)
        else:
            raise FileNotFoundError("System settings not found " + env_system)

        # System file path
        input_paths = wdir + "/Env_input_paths"
        if os.path.exists(input_paths):
            with open(input_paths, mode="r", encoding="utf-8") as input_paths:
                system_file_paths = json.load(input_paths)
        else:
            raise FileNotFoundError("System setting input paths not found " + input_paths)

        # Submission settings
        env_submit = wdir + "/Env_submit"
        if os.path.exists(env_submit):
            with open(env_submit, mode="r", encoding="utf-8") as env_submit:
                env_submit = json.load(env_submit)
        else:
            raise FileNotFoundError("Submision settings not found " + env_submit)

        # Scheduler settings
        env_server = wdir + "/Env_server"
        if os.path.exists(env_server):
            server = scheduler.EcflowServerFromFile(env_server)
        else:
            raise FileNotFoundError("Server settings missing " + env_server)

        # Date/time settings
        # TODO adapt progress object
        dtg = None
        dtgbeg = None
        dtgpp = None
        stream_txt = ""
        if stream is not None:
            stream_txt = f"_stream{stream}_"
        with open(f"{wdir}/progress{stream_txt}.json", mode="r", encoding="utf-8") as progress_file:
            progress = json.load(progress_file)
            dtg = progress["DTG"]
            dtgbeg = progress["DTGBEG"]
        with open(f"{wdir}/progressPP{stream_txt}.json", mode="r", encoding="utf-8") as progress_pp_file:
            progress_pp = json.load(progress_pp_file)
            dtgpp = progress_pp["DTGPP"]
        progress = {
            "DTG": dtg,
            "DTGBEG": dtgbeg,
            "DTGPP": dtgpp
        }

        # Configuration
        config_files_dict = self.get_config_files_dict(wdir)
        all_merged_settings = self.merge_dict_from_config_dicts(config_files_dict)

        # Stream
        all_merged_settings["GENERAL"].update({"STREAM": stream})

        # Geometry
        domains = wdir + "/config/domains/Harmonie_domains.json"
        if os.path.exists(domains):
            with open(domains, mode="r", encoding="utf-8") as domains:
                domains = json.load(domains)
                all_merged_settings["GEOMETRY"].update({"DOMAINS": domains})
        else:
            raise FileNotFoundError("Domains not found " + domains)

        # Scheduler
        all_merged_settings.update({"SCHEDULER": server.settings})
        # Submission
        all_merged_settings.update({"SUBMISSION": env_submit})
        # System path variables
        all_merged_settings.update({"SYSTEM_FILE_PATHS": system_file_paths})
        # System settings
        all_merged_settings.update({"SYSTEM_VARS": env_system})
        # Date/time
        all_merged_settings.update({"PROGRESS": progress})

        # Troika
        all_merged_settings.update({"TROIKA": { "CONFIG": wdir + "/config/troika_config.yml"}})

        # m_config, member_m_config = experiment_setup.process_merged_settings(all_merged_settings)

        Exp.__init__(self, exp_dependencies, all_merged_settings)

    @staticmethod
    def merge_dict_from_config_dicts(config_files):
        """Merge the settings in a config dict.

        Args:
            config_files (list): _description_

        Returns:
            _type_: _description_

        """
        logging.debug("config_files: %s", str(config_files))
        merged_env = {}
        for fff in config_files:
            # print(f)
            modification = config_files[fff]["toml"]
            merged_env = ExpConfiguration.merge_dict(merged_env, modification)
        return merged_env


    @staticmethod
    def get_config_files_dict(work_dir, pysurfex_experiment=None, pysurfex=None, must_exists=True):
        """Get the needed set of configurations files.

        Raises:
            FileNotFoundError: if no config file is found.

        Returns:
            dict: config_files_dict
        """
        config = ExpFromFiles.get_config(work_dir, pysurfex_experiment=pysurfex_experiment,
                                         must_exists=must_exists)
        c_files = config["config_files"]
        config_files_dict = {}
        for fname in c_files:
            lfile = work_dir + "/config/" + fname
            if fname == "config_exp_surfex.toml":
                if pysurfex is not None:
                    lfile = pysurfex + "/surfex/cfg/" + fname

            if os.path.exists(lfile):
                print("lfile", lfile)
                toml_dict = toml.load(open(lfile, mode="r", encoding="UTF-8"))
            else:
                raise FileNotFoundError("No config file found for " + lfile)

            config_files_dict.update({
                fname: {
                    "toml": toml_dict,
                    "blocks": config[fname]["blocks"]
                }
            })
        return config_files_dict

    @staticmethod
    def get_config(wdir, pysurfex_experiment=None, must_exists=True):
        """Get the config definiton.

        Args:
            wdir (str): work directory to search for config
            pysurfex_experiment (str, optional): path to pysurfex-experiment
            must_exists (bool, optional): raise exception on not existing. Defaults to True.

        Raises:
            Exception: _description_

        Returns:
            list: List of config files
        """
        # Check existence of needed config files
        local_config = wdir + "/config/config.toml"
        search_configs = [local_config]
        if pysurfex_experiment is not None:
            search_configs.append(pysurfex_experiment + "/config/config.toml")

        c_files = None
        for conf in search_configs:
            if c_files is None and os.path.exists(conf):
                with open(conf, mode="r", encoding="utf-8") as file_handler:
                    c_files = toml.load(file_handler)

        if must_exists and c_files is None:
            raise Exception(f"No config found in {str(search_configs)}")

        return c_files
