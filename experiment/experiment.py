import scheduler
import experiment
import experiment_setup
import os
import toml
import json
import shutil


class Exp(object):
    def __init__(self, exp_dependencies, merged_config, member_merged_config, submit_exceptions=None,
                 system_file_paths=None, system=None, server=None, env_submit=None, progress=None,
                 debug=False):
        """Instaniate an object of the main experiment class

        Args:
            exp_dependencies (dict):  Eperiment dependencies
            merged_config (dict): Experiment configuration
            member_merged_config (dict): Member (EPS) specific configuration settings
            submit_exceptions (dict):
            system_file_paths (experiment.SystemFilePaths):
            system (experiment.System:
            server (scheduler.Server):
            env_submit (dict):
            progress (dict):
            debug (bool): Write extra debug information
        """

        rev = exp_dependencies["revision"]
        pysurfex_experiment = exp_dependencies["pysurfex_experiment"]
        offline_source = exp_dependencies["offline_source"]
        pysurfex = exp_dependencies["pysurfex"]
        wdir = exp_dependencies["exp_dir"]
        exp_name = exp_dependencies["exp_name"]

        self.name = exp_name
        self.wd = wdir
        self.rev = rev
        self.pysurfex = pysurfex
        self.pysurfex_experiment = pysurfex_experiment
        self.offline_source = offline_source
        self.system = system
        self.server = server
        self.env_submit = env_submit
        self.submit_exceptions = submit_exceptions
        self.system_file_paths = system_file_paths
        self.progress = progress
        self.debug = debug

        self.config_dict = merged_config
        self.member_config = member_merged_config
        self.config = experiment.ExpConfiguration(merged_config, member_merged_config)

    def checkout(self, file):
        """

        """
        if file is None:
            raise Exception("File must be set")
        if os.path.exists(file):
            print("File is aleady checked out " + file)
        else:
            if os.path.exists(self.rev + "/" + file):
                dirname = os.path.dirname(self.wd + "/" + file)
                os.makedirs(dirname, exist_ok=True)
                shutil.copy2(self.rev + "/" + file, self.wd + "/" + file)
                print("Checked out file: " + file)
            else:
                print("File was not found: " + self.rev + "/" + file)

    def write_scheduler_info(self, logfile, debug=False):

        submit_exceptions = {}
        if self.submit_exceptions is not None:
            submit_exceptions = self.submit_exceptions
        exp_settings = {}
        exp_settings.update({"submit_exceptions": submit_exceptions})
        exp_settings.update({"env_file": self.wd + "/Env"})
        exp_settings.update({"env_submit": self.env_submit})
        exp_settings.update({"env_server": self.server.settings})
        hosts = self.system.hosts
        joboutdir = {}
        for host in range(0, len(hosts)):
            joboutdir.update({str(host): self.system.get_var("JOBOUTDIR", str(host))})
        exp_settings.update({"joboutdir": joboutdir})
        exp_settings.update({"logfile": logfile})
        if debug:
            print(__file__, exp_settings)
        json.dump(exp_settings, open(self.wd + "/scheduler.json", "w"), indent=2)

    def dump_exp_configuration(self, filename, indent=None):
        config = {
            "config": self.config_dict,
            "member_config": self.member_config
        }
        json.dump(config, open(filename, "w"), indent=indent)


class ExpFromFiles(Exp):
    def __init__(self, exp_dependencies_file, stream=None, progress=None, debug=False):

        if debug:
            print(__file__, "ExpFromFiles")

        exp_dependencies = json.load(open(exp_dependencies_file, "r"))
        pysurfex_experiment = exp_dependencies["pysurfex_experiment"]
        wdir = exp_dependencies["exp_dir"]
        self.wd = wdir
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
                raise Exception("System target file is missing " + target)

        c_files = self.get_config(wdir, pysurfex_experiment)["config_files"]
        config_files = []
        for f in c_files:
            lfile = wdir + "/config/" + f
            config_files.append(lfile)
            if not os.path.exists(lfile):
                raise Exception("Needed config file is missing " + f)

        # System
        env_system = wdir + "/Env_system"
        system = experiment.SystemFromFile(env_system, exp_name)
        # Dump system variables
        system.dump_system_vars(wdir + "/exp_system_vars.json", indent=2)

        # System file path
        input_paths = wdir + "/Env_input_paths"
        if os.path.exists(input_paths):
            system_file_paths = json.load(open(input_paths, "r"))
        else:
            raise FileNotFoundError("System setting input paths not found " + input_paths)
        system_file_paths = experiment.SystemFilePathsFromSystem(system_file_paths, system, hosts=system.hosts,
                                                                 stream=stream, wdir=wdir)
        # Dump experiment file paths
        system_file_paths.dump_system(wdir + "/exp_system.json", indent=2)

        env_submit = json.load(open(wdir + "/Env_submit", "r"))

        logfile = system.get_var("SFX_EXP_DATA", "0") + "/ECF.log"
        server = scheduler.EcflowServerFromFile(wdir + "/Env_server", logfile)

        if progress is None:
            progress = experiment.ProgressFromFile(wdir + "/progress.json",
                                                   wdir + "/progressPP.json")

        config_files = self.get_config_files()
        all_merged_settings = experiment_setup.merge_toml_env_from_config_dicts(config_files)
        merged_config, member_merged_config = experiment_setup.process_merged_settings(all_merged_settings)

        submit_exceptions = wdir + "/config/submit/submission.json"
        submit_exceptions = json.load(open(submit_exceptions, "r"))

        Exp.__init__(self, exp_dependencies, merged_config, member_merged_config,
                     submit_exceptions=submit_exceptions,
                     system_file_paths=system_file_paths, debug=debug,
                     system=system, server=server, env_submit=env_submit, progress=progress)
        self.config.dump_json(self.wd + "/exp_configuration.json", indent=2)

    def get_config_files(self):
        """Get the needed set of configurations files

        Returns:

        """

        # Check existence of needed config files
        config_file = self.wd + "/config/config.toml"
        config = toml.load(open(config_file, "r"))
        c_files = config["config_files"]
        config_files = {}
        for f in c_files:
            lfile = self.wd + "/config/" + f
            if os.path.exists(lfile):
                # print("lfile", lfile)
                toml_dict = toml.load(open(lfile, "r"))
            else:
                raise Exception("No config file found for " + lfile)

            config_files.update({
                f: {
                    "toml": toml_dict,
                    "blocks": config[f]["blocks"]
                }
            })
        return config_files

    @staticmethod
    def get_config(wdir, pysurfex_experiment, must_exists=True):
        # Check existence of needed config files
        local_config = wdir + "/config/config.toml"
        exp_config = pysurfex_experiment + "/config/config.toml"
        if os.path.exists(local_config):
            c_files = toml.load(open(local_config, "r"))
        elif os.path.exists(exp_config):
            c_files = toml.load(open(exp_config, "r"))
        else:
            if must_exists:
                raise Exception("No config found in " + local_config + " or " + exp_config)
            else:
                c_files = None
        return c_files
