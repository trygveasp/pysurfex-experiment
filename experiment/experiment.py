"""Experiment classes and methods."""
import os
import json
import logging
import scheduler
import toml
import experiment
import experiment_setup


class Exp():
    """Experiment class."""

    def __init__(self, exp_dependencies, merged_config, member_merged_config,
                 submit_exceptions=None, system_file_paths=None, system=None,
                 server=None, env_submit=None, progress=None):
        """Instaniate an object of the main experiment class.

        Args:
            exp_dependencies (dict):  Eperiment dependencies
            merged_config (dict): Experiment configuration
            member_merged_config (dict): Member (EPS) specific configuration settings
            submit_exceptions (dict):
            system_file_paths (experiment.SystemFilePaths):
            system (experiment.System):
            server (scheduler.Server):
            env_submit (dict):
            progress (dict):

        """
        logging.debug("Construct Exp")
        offline_source = exp_dependencies.get("offline_source")
        wdir = exp_dependencies.get("exp_dir")
        exp_name = exp_dependencies.get("exp_name")

        self.name = exp_name
        merged_config["GENERAL"].update({"EXP": exp_name})
        self.work_dir = wdir
        self.scripts = exp_dependencies.get("pysurfex_experiment")
        self.pysurfex = exp_dependencies.get("pysurfex")
        self.offline_source = offline_source
        self.system = system
        self.server = server
        self.env_submit = env_submit
        self.submit_exceptions = submit_exceptions
        self.system_file_paths = system_file_paths
        self.progress = progress

        self.config_dict = merged_config
        self.member_config = member_merged_config
        self.config = experiment.ExpConfiguration(merged_config, member_merged_config)
        self.config_file = None

    def write_scheduler_info(self, logfile, filename=None):
        """Write scheduler info.

        Args:
            logfile (str): Filename
            filename (_type_, optional): _description_. Defaults to None.
        """
        if filename is None:
            filename = self.work_dir + "/scheduler.json"
        exp_settings = self.create_scheduler_info(logfile)
        with open(filename, mode="w", encoding="UTF-8") as file_handler:
            json.dump(exp_settings, file_handler, indent=2)

    def create_scheduler_info(self, logfile):
        """Create the scheduler info."""
        submit_exceptions = {}
        if self.submit_exceptions is not None:
            submit_exceptions = self.submit_exceptions
        exp_settings = {}
        exp_settings.update({"submit_exceptions": submit_exceptions})
        exp_settings.update({"env_file": self.work_dir + "/Env"})
        exp_settings.update({"env_submit": self.env_submit})
        exp_settings.update({"env_server": self.server.settings})
        hosts = self.system.hosts
        joboutdir = {}
        for host in range(0, len(hosts)):
            joboutdir.update({str(host): self.system.get_var("JOBOUTDIR", str(host))})
        exp_settings.update({"joboutdir": joboutdir})
        exp_settings.update({"logfile": logfile})
        logging.debug("create_scheduler_info: %s", str(exp_settings))
        return exp_settings

    def dump_exp_configuration(self, filename, indent=None):
        """Dump the exp configuration.

        The configuration has two keys. One for general config and one for member config.

        Args:
            filename (str): filename to dump to
            indent (int, optional): indentation in json file. Defaults to None.
        """
        config = {
            "config": self.config_dict,
            "member_config": self.member_config
        }
        self.config_file = filename
        json.dump(config, open(filename, mode="w", encoding="UTF-8"), indent=indent)


class ExpFromFiles(Exp):
    """Generate Exp object from existing files. Use config files from a setup."""

    def __init__(self, exp_dependencies_file, stream=None, progress=None):
        """Construct an Exp object from files.

        Args:
            exp_dependencies_file (_type_): _description_
            stream (_type_, optional): _description_. Defaults to None.
            progress (_type_, optional): _description_. Defaults to None.

        Raises:
            FileNotFoundError: _description_
            FileNotFoundError: _description_

        """
        logging.debug("Construct ExpFromFiles")
        exp_dependencies = json.load(open(exp_dependencies_file, mode="r", encoding="UTF-8"))
        wdir = exp_dependencies.get("exp_dir")
        self.work_dir = wdir
        exp_name = exp_dependencies["exp_name"]

        # Check existence of needed system files
        system_files = {
            "Env_system": "",
            "Env": "",
            "Env_submit": ""
        }

        # Check for needed system files
        for key in system_files:
            target = wdir + "/" + key
            if not os.path.exists(target):
                raise FileNotFoundError("System target file is missing " + target)

        # System
        env_system = wdir + "/Env_system"
        system = experiment.SystemFromFile(env_system, exp_name)
        # Dump system variables
        system.dump_system_vars(wdir + "/exp_system_vars.json", indent=2)

        # System file path
        input_paths = wdir + "/Env_input_paths"
        if os.path.exists(input_paths):
            system_file_paths = json.load(open(input_paths, mode="r", encoding="UTF-8"))
        else:
            raise FileNotFoundError("System setting input paths not found " + input_paths)
        system_file_paths = experiment.SystemFilePathsFromSystem(system_file_paths, system,
                                                                 hosts=system.hosts,
                                                                 stream=stream, wdir=wdir)
        # Dump experiment file paths
        system_file_paths.dump_system(wdir + "/exp_system.json", indent=2)

        env_submit = json.load(open(wdir + "/Env_submit", mode="r", encoding="UTF-8"))

        # logfile = system.get_var("SFX_EXP_DATA", "0") + "/ECF.log"
        server = scheduler.EcflowServerFromFile(wdir + "/Env_server")

        if progress is None:
            progress = experiment.ProgressFromFile(wdir + "/progress.json",
                                                   wdir + "/progressPP.json")

        # Configuration
        config_files_dict = self.get_config_files_dict(wdir)
        all_merged_settings = experiment_setup.merge_toml_env_from_config_dicts(config_files_dict)

        # Geometry
        domains = wdir + "/config/domains/Harmonie_domains.json"
        domains = json.load(open(domains, mode="r", encoding="UTF-8"))
        all_merged_settings["GEOMETRY"].update({"DOMAINS": domains})
        m_config, member_m_config = experiment_setup.process_merged_settings(all_merged_settings)

        # Submission exceptions
        submit_exceptions = wdir + "/config/submit/submission.json"
        submit_exceptions = json.load(open(submit_exceptions, mode="r", encoding="UTF-8"))

        Exp.__init__(self, exp_dependencies, m_config, member_m_config,
                     submit_exceptions=submit_exceptions,
                     system_file_paths=system_file_paths,
                     system=system, server=server, env_submit=env_submit, progress=progress)
        self.config.dump_json(self.work_dir + "/exp_configuration.json", indent=2)

    @staticmethod
    def get_config_files_dict(work_dir, pysurfex_experiment=None, pysurfex=None, must_exists=True):
        """Get the needed set of configurations files.

        Raises:
            FileNotFoundError: if no config file is found.

        Returns:
            dict: config_files_dict
        """
        # Check existence of needed config files
        # config_file = work_dir + "/config/config.toml"
        # config = toml.load(open(config_file, mode="r", encoding="UTF-8"))
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


class ExpFromSetupFiles(Exp):
    """Experiment class."""

    def __init__(self, exp_dependencies_file):

        with open(exp_dependencies_file, mode="r", encoding="utf-8") as exp_dep:
            exp_dependencies = json.load(exp_dep)
            exp_configuration = exp_dependencies["exp_configuration"]

        with open(exp_configuration, mode="r", encoding="utf-8") as exp_conf:
            config_file = exp_config["config_file"]

            config = json.load(open(config_file, mode="r", encoding="utf-8"))
            member_config_file = exp_dependencies["member_config_file"]
            member_config = json.load(open(member_config_file, mode="r", encoding="utf-8"))
            submit_exceptions = exp_dependencies_file["submit_exceptions"]

        Exp.__init__(self, exp_dependencies, config, member_config,
                     submit_exceptions=submit_exceptions,
                     system_file_paths=system_file_paths, system=system,
                     server=server, env_submit=env_submit, progress=progress)
