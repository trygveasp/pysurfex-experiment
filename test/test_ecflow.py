"""Unit testing."""
import os
import sys
import unittest
from pathlib import Path
import json
import inspect
import shutil
import logging
import toml
import scheduler
import experiment
import experiment_setup
import experiment_tasks
import ecf


TESTDATA = f"{str((Path(__file__).parent).parent)}/testdata"
ROOT = f"{str((Path(__file__).parent).parent)}"
logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                    level=logging.DEBUG)


class TestEcflow(unittest.TestCase):
    """Test ecflow tasks."""

    @classmethod
    def setUpClass(cls):
        """Do initialization once."""
        print("Initialization done once. Sets up the default experiment.")
        rm_dirs = ["/tmp/host0", "/tmp/host1"]
        mk_dirs = ["/tmp/host0", "/tmp/host1"]
        touch_files = [
            "/tmp/host1/testdata/input_paths/ecoclimap_bin_dir/ecoclimapI_covers_param.bin",
            "/tmp/host1/testdata/input_paths/ecoclimap_bin_dir/ecoclimapII_af_covers_param.bin",
            "/tmp/host1/testdata/input_paths/ecoclimap_bin_dir/ecoclimapII_eu_covers_param.bin",
            "/tmp/host1/testdata/input_paths/flake_dir/GlobalLakeDepth_V3.0.dir",
            "/tmp/host1/testdata/input_paths/flake_dir/GlobalLakeStatus_V3.0.dir",
            "/tmp/host1/testdata/input_paths/flake_dir/LAKE_LTA_NEW.nc",
            "/tmp/host1/testdata/input_paths/sand_dir/sand_fao.dir",
            "/tmp/host1/testdata/input_paths/sand_dir/sand_fao.hdr",
            "/tmp/host1/testdata/input_paths/clay_dir/clay_fao.dir",
            "/tmp/host1/testdata/input_paths/clay_dir/clay_fao.hdr",
            "/tmp/host1/testdata/input_paths/soc_top_dir/soc_top.dir"
            "/tmp/host1/testdata/input_paths/soc_top_dir/soc_top.hdr",
            "/tmp/host1/testdata/input_paths/soc_sub_dir/soc_sub.dir",
            "/tmp/host1/testdata/input_paths/soc_sub_dir/soc_sub.hdr",
            "/tmp/host1/testdata/input_paths/ecoclimap_cover_dir/ECOCLIMAP_2_5_p.dir",
            "/tmp/host1/testdata/input_paths/oro_dir/gmted2010.dir",
            "/tmp/host1/testdata/input_paths/oro_dir/gmted2010.hdr"
        ]
        copy_dirs = {
            TESTDATA: "/tmp/host0/testdata",
            "/tmp/host0/testdata": "/tmp/host1/testdata",
        }

        for dname in rm_dirs:
            if os.path.exists(dname):
                shutil.rmtree(dname)

        for dname in mk_dirs:
            os.makedirs(dname, exist_ok=True)

        for target, dest in copy_dirs.items():
            destination = dest + "/"
            if os.path.exists(target):
                # os.makedirs(destination, exist_ok=True)
                shutil.copytree(target, destination)

        for fname in touch_files:
            dname = os.path.abspath(os.path.dirname(fname))
            os.makedirs(dname, exist_ok=True)
            os.system("touch " + fname)

        conf_files = toml.load("pysurfex-experiment/config/config.toml")["config_files"]
        config_files = []
        for fname in conf_files:
            dirname = "pysurfex-experiment/config"
            if fname == "config_exp_surfex.toml":
                dirname = "pysurfex/surfex/cfg"
            config_files.append(f"{dirname}/{fname}")
        logging.debug("config_files: %s", str(config_files))
        default_config = experiment_setup.merge_toml_env_from_files(config_files)

        print("scheduler module: ", scheduler.__file__)
        print("experiment module: ", experiment.__file__)
        cls.exp_name = "TEST_ECFLOW"

        cls.wdir = f"/tmp/host0/sfx_home/{cls.exp_name}"

        rev = ROOT
        pysurfex_path = f"{ROOT}/../pysurfex"

        os.makedirs(cls.wdir, exist_ok=True)
        argv = [
            "--wd", cls.wdir,
            "-surfex", pysurfex_path,
            "-rev", rev,
            "-host", "unittest",
            "--debug"
        ]
        kwargs = experiment_setup.parse_surfex_script_setup(argv)
        experiment_setup.surfex_script_setup(**kwargs)

        argv = [
            "--wd", cls.wdir,
            "--debug"
        ]
        kwargs = experiment.parse_update_config(argv)
        experiment.update_config(**kwargs)

        argv = [
            "--wd", cls.wdir,
            "--debug",
            "start",
            "-dtg", "2022042803",
            "-dtgend", "2022042806"
        ]
        kwargs = experiment.parse_surfex_script(argv)
        experiment.surfex_script(**kwargs)

        cls.system_dict = toml.load(open(f"{cls.wdir}/Env_system", mode="r", encoding="utf-8"))

        exp_dependencies_file = f"{cls.wdir}/paths_to_sync.json"
        cls.exp_dependencies = json.load(open(exp_dependencies_file, mode="r", encoding="utf-8"))

        sfx_exp = experiment.ExpFromFiles(exp_dependencies_file, stream=None)
        sfx_exp.write_scheduler_info(f"{cls.wdir}/scheduler.json")

        cls.input_files = json.load(open(f"{cls.wdir}/exp_system.json", mode="r", encoding="utf-8"))

        # Sync data. To InitRun
        lib_dir = cls.input_files["0"]["sfx_exp_lib"]
        sys.path.insert(0, lib_dir)

        system_vars_file = f"{cls.wdir}/exp_system_vars.json"
        experiment_setup.init_run_from_file(system_vars_file, exp_dependencies_file)

        cls.system_vars = json.load(open(system_vars_file, mode="r", encoding="utf-8"))
        server_logfile = f'{cls.system_vars["0"]["SFX_EXP_DATA"]}/ECF.log'
        cls.ecflow_vars = {
            "LIB": "%LIB%",
            "HOST": "@HOST_TO_BE_SUBSTITUTED@",
            "SERVER_LOGFILE": server_logfile,
            "WRAPPER": "wrapper",
            "MBR": "",
            "DTG": "2022042806",
            "NEXT_DTG": "2022042806",
            "DTGBEG": "2022042803",
            "stream": "",
            "TASK_NAME": "%TASK%",
            "DEBUG": True,
            "FORCE": False,
            "CHECK_EXISTENCE": True,
            "PRINT_NAMELIST": True,
            "STREAM": "",
            "ARGS": "var1=val1;var2=val2",
            "ECF_NAME": "%ECF_NAME%",
            "ECF_PASS": "pass",
            "ECF_TRYNO": "1",
            "ECF_RID": "12345",
            "SUBMISSION_ID": "12345%"
        }

        cls.server_settings = json.load(open(f"{cls.wdir}/Env_server", mode="r",
                                        encoding="utf-8"))
        print(cls.server_settings)
        cls.exp_config = json.load(open(f"{cls.wdir}/exp_configuration.json", mode="r",
                                   encoding="utf-8"))

        cls.system = experiment.System(cls.system_dict, cls.exp_name)
        cls.env_submit = json.load(open(f"{cls.wdir}/Env_submit", mode="r", encoding="utf-8"))

        domains = {
            "DRAMMEN": {
                "GSIZE": 2500.0,
                "LAT0": 60.0,
                "LATC": 60.0,
                "LON0": 10.0,
                "LONC": 10.0,
                "NLAT": 60,
                "NLON": 50,
                "TSTEP": 600,
                "EZONE": 0
            }
        }
        test_config = {
            "GENERAL": {
                "HH_LIST": "0-12:3,18-23:1"
            },
            "GEOMETRY": {
                "DOMAIN": "DRAMMEN",
                "DOMAINS": domains
            }
        }
        cls.merged_config = experiment_setup.merge_toml_env(default_config, test_config)
        cls.progress = {
            "DTG": "2022042806",
            "DTGBEG": "2022042803"
        }
        cls.task = scheduler.EcflowTask("/test/Task", 1, "ecf_pass", 11, None)

        ecf.InitRun.parse_ecflow_vars_init_run()
        lib = cls.wdir
        system_vars = ecf.InitRun.read_system_vars_init_run(lib)
        server_settings = ecf.InitRun.read_ecflow_server_file_init_run(lib)
        exp_dependencies = ecf.InitRun.read_paths_to_sync_init_run(lib)
        ecf.init_run_main(system_vars, server_settings, exp_dependencies, **cls.ecflow_vars)

    def test_logprogress_pp_ecf_task(self):
        """Test log progress ecflow container for pp family."""
        ecf.LogProgressPP.parse_ecflow_vars_logprogress_pp()
        ecflow_vars = self.ecflow_vars.copy()
        ecflow_vars.update({
            "LIB": self.wdir,
            "HOST": "0",
            "TASK_NAME": "LogProgressPP",
            "ECF_NAME": "/suite/family/LogProgressPP"
        })
        lib = self.system_vars["0"]["SFX_EXP_LIB"]
        server_settings = ecf.LogProgress.read_ecflow_server_file_logprogress(lib)
        ecf.log_progress_pp_main(server_settings, **ecflow_vars)

    def test_default_ecf_task(self):
        """Test default ecflow container."""
        host = "0"
        ensmbr = None
        lib = self.system_vars["0"]["SFX_EXP_LIB"]
        ecf.default.parse_ecflow_vars()
        ecflow_vars = self.ecflow_vars.copy()
        ecflow_vars.update({
            "LIB": lib,
            "HOST": host,
            "TASK_NAME": "Dummy",
            "ECF_NAME": "/suite/family/default"
        })
        system_vars = ecf.default.read_system_vars(lib, host=host)
        server_settings = ecf.default.read_ecflow_server_file(lib)
        exp_config = ecf.default.read_exp_configuration(lib, ensmbr=ensmbr)
        input_files = ecf.default.read_system_file_paths(lib, host=host)
        ecf.default_main(system_vars, server_settings, exp_config, input_files, **ecflow_vars)

    def test_logprogress_ecf_task(self):
        """Test log progress ecflow container."""
        ecf.LogProgress.parse_ecflow_vars_logprogress()
        ecflow_vars = self.ecflow_vars.copy()
        ecflow_vars.update({
            "LIB": self.wdir,
            "HOST": "0",
            "TASK_NAME": "LogProgress",
            "ECF_NAME": "/suite/family/LogProgress"
        })
        lib = self.system_vars["0"]["SFX_EXP_LIB"]
        server_settings = ecf.LogProgress.read_ecflow_server_file_logprogress(lib)
        ecf.log_progress_main(server_settings, **ecflow_vars)

    def test_task_from_inspect(self):
        """Test task."""
        stream = None

        test_hosts = ["0", "1"]
        for host in test_hosts:

            lib_dir = self.system.get_var("SFX_EXP_LIB", host)
            data_dir = self.system.get_var("SFX_EXP_DATA", host)
            self.assertEqual(data_dir, f"/tmp/host{host}/scratch/sfx_home/{self.exp_name}")

            env_input = json.load(open(f"{lib_dir}/Env_input_paths", mode="r", encoding="utf-8"))
            system_file_paths = experiment.SystemFilePathsFromSystem(env_input, self.system,
                                                                     hosts=self.system.hosts,
                                                                     stream=stream,
                                                                     wdir=self.wdir).paths[host]

            merged_config, member_config = \
                experiment_setup.process_merged_settings(self.merged_config)
            exp = experiment.Exp(self.exp_dependencies, merged_config, member_config,
                                 submit_exceptions=None, system_file_paths=system_file_paths,
                                 system=self.system, server=None, env_submit=self.env_submit,
                                 progress=None)

            # task = scheduler.EcflowTask("/test/Prep", 1, "ecf_pass", 11, None)
            config = exp.config.settings
            system = exp.system.system[host]
            exp_file_paths = exp.system_file_paths

            kwargs = {}
            classes = inspect.getmembers(experiment_tasks, inspect.isclass)
            for cinfo in classes:
                cname = cinfo[0]
                cclass = cinfo[1]
                print(cname)
                tests = ["PrepareCycle", "FirstGuess", "CycleFirstGuess"]
                if cname in tests:
                    print("\n\n\n\n****************************\n")
                    print(cname)
                    cclass(self.task, config, system, exp_file_paths, self.progress,
                           **kwargs).run()

    def test_pgd(self):
        """Test task."""
        stream = None

        test_hosts = ["0", "1"]
        for host in test_hosts:

            lib_dir = self.system.get_var("SFX_EXP_LIB", host)
            data_dir = self.system.get_var("SFX_EXP_DATA", host)

            output = f"{data_dir}/climate/PGD.nc"
            if os.path.exists(output):
                os.unlink(output)

            env_input = json.load(open(f"{lib_dir}/Env_input_paths", mode="r", encoding="utf-8"))
            bin_dir = f"{lib_dir}/test/bin"
            env_input.update({
                "bin_dir": bin_dir
            })

            system_file_paths = experiment.SystemFilePathsFromSystem(env_input, self.system,
                                                                     hosts=self.system.hosts,
                                                                     stream=stream,
                                                                     wdir=self.wdir).paths[host]

            merged_config, member_config = \
                experiment_setup.process_merged_settings(self.merged_config)
            exp = experiment.Exp(self.exp_dependencies, merged_config, member_config,
                                 submit_exceptions=None, system_file_paths=system_file_paths,
                                 system=self.system, server=None, env_submit=self.env_submit,
                                 progress=None)

            config = exp.config.settings
            system = exp.system.system[host]
            exp_file_paths = exp.system_file_paths
            kwargs = {}
            experiment_tasks.Pgd(self.task, config, system, exp_file_paths, self.progress,
                                 **kwargs).run()
            self.assertTrue(os.path.exists(output))

    def test_prep(self):
        """Test task."""
        stream = None

        test_hosts = ["0", "1"]
        for host in test_hosts:

            lib_dir = self.system.get_var("SFX_EXP_LIB", host)
            data_dir = self.system.get_var("SFX_EXP_DATA", host)

            prep_dir = f"{data_dir}/archive/2022/04/28/06/"
            output = f"{prep_dir}/PREP.nc"
            if os.path.exists(output):
                os.unlink(output)
            env_input = json.load(open(f"{lib_dir}/Env_input_paths", mode="r", encoding="utf-8"))
            clim_dir = "/tmp/host" + host + "/testdata/climate/"
            bin_dir = f"{lib_dir}/test/bin"
            env_input.update({
                "pgd_dir": clim_dir,
                "prep_dir": prep_dir,
                "bin_dir": bin_dir
            })

            system_file_paths = experiment.SystemFilePathsFromSystem(env_input, self.system,
                                                                     hosts=self.system.hosts,
                                                                     stream=stream,
                                                                     wdir=self.wdir).paths[host]

            merged_config, member_config = \
                experiment_setup.process_merged_settings(self.merged_config)
            exp = experiment.Exp(self.exp_dependencies, merged_config, member_config,
                                 submit_exceptions=None, system_file_paths=system_file_paths,
                                 system=self.system, server=None, env_submit=self.env_submit,
                                 progress=None)

            config = exp.config.settings
            system = exp.system.system[host]
            exp_file_paths = exp.system_file_paths
            kwargs = {}
            experiment_tasks.Prep(self.task, config, system, exp_file_paths, self.progress,
                                  **kwargs).run()
            self.assertTrue(os.path.exists(output))

    def test_forcing(self):
        """Test task."""
        stream = None

        test_hosts = ["1"]
        for host in test_hosts:

            lib_dir = self.system.get_var("SFX_EXP_LIB", host)
            data_dir = self.system.get_var("SFX_EXP_DATA", host)

            forcing_dir = f"{data_dir}/forcing/2022042806/"
            output = f"{forcing_dir}/FORCING.nc"
            if os.path.exists(output):
                os.unlink(output)

            env_input = json.load(open(f"{lib_dir}/Env_input_paths", mode="r", encoding="utf-8"))
            bin_dir = f"{lib_dir}/test/bin"
            env_input.update({
                "bin_dir": bin_dir,
                "forcing_dir": forcing_dir
            })

            test_config = {
                "GENERAL": {
                    "HH_LIST": "0-23:1"
                }
            }
            merged_config = experiment_setup.merge_toml_env(self.merged_config, test_config)

            system_file_paths = experiment.SystemFilePathsFromSystem(env_input, self.system,
                                                                     hosts=self.system.hosts,
                                                                     stream=stream,
                                                                     wdir=self.wdir).paths[host]

            merged_config, member_config = \
                experiment_setup.process_merged_settings(self.merged_config)
            exp = experiment.Exp(self.exp_dependencies, merged_config, member_config,
                                 submit_exceptions=None, system_file_paths=system_file_paths,
                                 system=self.system, server=None, env_submit=self.env_submit,
                                 progress=None)

            config = exp.config.settings
            system = exp.system.system[host]
            exp_file_paths = exp.system_file_paths

            kwargs = {}
            experiment_tasks.Forcing(self.task, config, system, exp_file_paths, self.progress,
                                     **kwargs).run()
            self.assertTrue(os.path.exists(output))

    def test_forecast(self):
        """Test task."""
        stream = None

        test_hosts = ["0", "1"]
        for host in test_hosts:

            lib_dir = self.system.get_var("SFX_EXP_LIB", host)
            data_dir = self.system.get_var("SFX_EXP_DATA", host)

            output = f"{data_dir}/archive/2022/04/28/06/SURFOUT.nc"
            if os.path.exists(output):
                os.unlink(output)

            os.makedirs(f"{data_dir}/20220428_06", exist_ok=True)
            os.system(f"touch {data_dir}/20220428_06/fc_start_sfx")

            env_input = json.load(open(f"{lib_dir}/Env_input_paths", mode="r", encoding="utf-8"))
            clim_dir = "/tmp/host" + host + "/testdata/climate/"
            first_guess_dir = "/tmp/host" + host + "/testdata/archive/@YYYY@/@MM@/@DD@/@HH@/@EEE@"
            forcing_dir = "/tmp/host" + host + "/testdata/forcing/2022042803/"
            bin_dir = f"{lib_dir}/test/bin"
            env_input.update({
                "pgd_dir": clim_dir,
                "prep_dir": first_guess_dir,
                "forcing_dir": forcing_dir,
                "bin_dir": bin_dir
            })

            system_file_paths = experiment.SystemFilePathsFromSystem(env_input, self.system,
                                                                     hosts=self.system.hosts,
                                                                     stream=stream,
                                                                     wdir=self.wdir).paths[host]

            merged_config, member_config = \
                experiment_setup.process_merged_settings(self.merged_config)
            exp = experiment.Exp(self.exp_dependencies, merged_config, member_config,
                                 submit_exceptions=None, system_file_paths=system_file_paths,
                                 system=self.system, server=None, env_submit=self.env_submit,
                                 progress=None)

            config = exp.config.settings
            system = exp.system.system[host]
            exp_file_paths = exp.system_file_paths
            kwargs = {}
            experiment_tasks.Forecast(self.task, config, system, exp_file_paths, self.progress,
                                      **kwargs).run()
            self.assertTrue(os.path.exists(output))

    def test_soda(self):
        """Test task."""
        stream = None

        test_hosts = ["0", "1"]
        for host in test_hosts:

            lib_dir = self.system.get_var("SFX_EXP_LIB", host)
            data_dir = self.system.get_var("SFX_EXP_DATA", host)

            output = f"{data_dir}/archive/2022/04/28/06/ANALYSIS.nc"
            if os.path.exists(output):
                os.unlink(output)

            os.makedirs(f"{data_dir}/20220428_06", exist_ok=True)
            os.system(f"touch {data_dir}/20220428_06/first_guess_sfx")

            env_input = json.load(open(f"{lib_dir}/Env_input_paths", mode="r", encoding="utf-8"))
            clim_dir = "/tmp/host" + host + "/testdata/climate/"
            first_guess_dir = "/tmp/host" + host + "/testdata/archive/@YYYY@/@MM@/@DD@/@HH@/@EEE@"
            obs_dir = "/tmp/host" + host + \
                      "/testdata/archive/observations/@YYYY@/@MM@/@DD@/@HH@/@EEE@"
            bin_dir = f"{lib_dir}/test/bin"
            env_input.update({
                "pgd_dir": clim_dir,
                "prep_dir": first_guess_dir,
                "first_guess_dir": first_guess_dir,
                "assim_dir": first_guess_dir,
                "obs_dir": obs_dir,
                "bin_dir": bin_dir
            })

            system_file_paths = experiment.SystemFilePathsFromSystem(env_input, self.system,
                                                                     hosts=self.system.hosts,
                                                                     stream=stream,
                                                                     wdir=self.wdir).paths[host]

            # Turn off ISBA assimilation for now in testing
            test_config = {
                "SURFEX": {
                    "ASSIM": {
                        "SCHEMES": {
                            "ISBA": "NONE"
                        }
                    }
                }
            }
            merged_config = experiment_setup.merge_toml_env(self.merged_config, test_config)
            merged_config, member_config = \
                experiment_setup.process_merged_settings(merged_config)
            exp = experiment.Exp(self.exp_dependencies, merged_config, member_config,
                                 submit_exceptions=None, system_file_paths=system_file_paths,
                                 system=self.system, server=None, env_submit=self.env_submit,
                                 progress=None)

            config = exp.config.settings
            system = exp.system.system[host]
            exp_file_paths = exp.system_file_paths
            kwargs = {}
            experiment_tasks.Soda(self.task, config, system, exp_file_paths, self.progress,
                                  **kwargs).run()
            self.assertTrue(os.path.exists(output))

    def test_first_guess4oi(self):
        """Test task."""
        stream = None

        test_hosts = ["1"]
        for host in test_hosts:

            lib_dir = self.system.get_var("SFX_EXP_LIB", host)
            data_dir = self.system.get_var("SFX_EXP_DATA", host)

            output = f"{data_dir}/archive/2022/04/28/06/raw.nc"
            if os.path.exists(output):
                os.unlink(output)

            os.makedirs(f"{data_dir}/20220428_06", exist_ok=True)
            os.makedirs(f"{data_dir}/archive/2022/04/28/06/", exist_ok=True)
            os.system(f"touch {data_dir}/20220428_06/first_guess_sfx")

            env_input = json.load(open(f"{lib_dir}/Env_input_paths", mode="r", encoding="utf-8"))
            clim_dir = "/tmp/host" + host + "/testdata/climate/"
            first_guess_dir = "/tmp/host" + host + "/testdata/archive/@YYYY@/@MM@/@DD@/@HH@/@EEE@"
            bin_dir = f"{lib_dir}/test/bin"
            env_input.update({
                "pgd_dir": clim_dir,
                "prep_dir": first_guess_dir,
                "first_guess_dir": first_guess_dir,
                "assim_dir": first_guess_dir,
                "bin_dir": bin_dir
            })

            system_file_paths = experiment.SystemFilePathsFromSystem(env_input, self.system,
                                                                     hosts=self.system.hosts,
                                                                     stream=stream,
                                                                     wdir=self.wdir).paths[host]

            merged_config, member_config = \
                experiment_setup.process_merged_settings(self.merged_config)
            exp = experiment.Exp(self.exp_dependencies, merged_config, member_config,
                                 submit_exceptions=None, system_file_paths=system_file_paths,
                                 system=self.system, server=None, env_submit=self.env_submit,
                                 progress=None)

            config = exp.config.settings
            system = exp.system.system[host]
            exp_file_paths = exp.system_file_paths
            kwargs = {}
            experiment_tasks.FirstGuess4OI(self.task, config, system, exp_file_paths, self.progress,
                                           **kwargs).run()
            self.assertTrue(os.path.exists(output))

    def test_qualitycontrol(self):
        """Test task."""
        stream = None

        test_hosts = ["1"]
        for host in test_hosts:

            lib_dir = self.system.get_var("SFX_EXP_LIB", host)
            data_dir = self.system.get_var("SFX_EXP_DATA", host)

            root = f"{data_dir}/test/qualitycontrol/"
            os.system(f"rm -r {root}")
            obs_dir = f"{root}/archive/observations/2022/04/28/06/"
            obs_dir_pattern = f"{root}/archive/observations/@YYYY@/@MM@/@DD@/@HH@/@EEE@"
            os.system(f"mkdir -p {obs_dir}")
            os.system(f"cp -r {TESTDATA}/archive/observations/2022/04/28/06/ob* {obs_dir}/.")

            first_guess_dir_pattern = f"{root}/archive/@YYYY@/@MM@/@DD@/@HH@/@EEE@"
            archive_dir_pattern = first_guess_dir_pattern
            archive_dir = f"{root}/archive/2022/04/28/06/"
            os.system(f"mkdir -p {archive_dir}")
            os.system(f"cp {TESTDATA}/archive/2022/04/28/06/raw*.nc {archive_dir}/.")

            env_input = json.load(open(f"{lib_dir}/Env_input_paths", mode="r", encoding="utf-8"))
            env_input.update({
                "first_guess_dir": first_guess_dir_pattern,
                "archive_dir": archive_dir_pattern,
                "obs_dir": obs_dir_pattern
            })

            system_file_paths = experiment.SystemFilePathsFromSystem(env_input, self.system,
                                                                     hosts=self.system.hosts,
                                                                     stream=stream,
                                                                     wdir=self.wdir).paths[host]

            merged_config, member_config = \
                experiment_setup.process_merged_settings(self.merged_config)
            exp = experiment.Exp(self.exp_dependencies, merged_config, member_config,
                                 submit_exceptions=None, system_file_paths=system_file_paths,
                                 system=self.system, server=None, env_submit=self.env_submit,
                                 progress=None)

            config = exp.config.settings
            system = exp.system.system[host]
            exp_file_paths = exp.system_file_paths

            kwargs = {}

            test_vars = {
                "t2m": "air_temperature_2m",
                "rh2m": "relative_humidity_2m",
                "sd": "surface_snow_thickness"
            }
            for var, var_name in test_vars.items():
                logging.debug("%s %s", var, var_name)
                output = f"{obs_dir}/qc_{var_name}.json"
                if os.path.exists(output):
                    os.unlink(output)
                task = scheduler.EcflowTask(f"/test/{var}/Task", 1, "ecf_pass", 11, None)
                experiment_tasks.QualityControl(task, config, system, exp_file_paths,
                                                self.progress,
                                                **kwargs).run()
                self.assertTrue(os.path.exists(output))

    def test_optimalinterpolation(self):
        """Test task."""
        stream = None

        test_hosts = ["1"]
        for host in test_hosts:

            lib_dir = self.system.get_var("SFX_EXP_LIB", host)
            data_dir = self.system.get_var("SFX_EXP_DATA", host)

            # Prepare data
            root = f"{data_dir}/test/optimalinterpolation/"
            os.system(f"rm -r {root}")
            obs_dir = f"{root}/archive/observations/2022/04/28/06/"
            obs_dir_pattern = f"{root}/archive/observations/@YYYY@/@MM@/@DD@/@HH@/@EEE@"
            os.system(f"mkdir -p {obs_dir}")

            archive_dir = f"{root}/archive/2022/04/28/06/"

            archive_dir_pattern = f"{root}/archive/@YYYY@/@MM@/@DD@/@HH@/@EEE@"
            first_guess_dir_pattern = archive_dir_pattern
            os.system(f"mkdir -p {archive_dir}")
            os.system(f"cp -r {TESTDATA}/archive/observations/2022/04/28/06/qc_*.json {obs_dir}/.")
            os.system(f"cp -r {TESTDATA}/archive/2022/04/28/06/raw*.nc {archive_dir}/.")

            env_input = json.load(open(f"{lib_dir}/Env_input_paths", mode="r", encoding="utf-8"))
            env_input.update({
                "first_guess_dir": first_guess_dir_pattern,
                "archive_dir": archive_dir_pattern,
                "obs_dir": obs_dir_pattern
            })

            system_file_paths = experiment.SystemFilePathsFromSystem(env_input, self.system,
                                                                     hosts=self.system.hosts,
                                                                     stream=stream,
                                                                     wdir=self.wdir).paths[host]

            merged_config, member_config = \
                experiment_setup.process_merged_settings(self.merged_config)
            exp = experiment.Exp(self.exp_dependencies, merged_config, member_config,
                                 submit_exceptions=None, system_file_paths=system_file_paths,
                                 system=self.system, server=None, env_submit=self.env_submit,
                                 progress=None)

            config = exp.config.settings
            system = exp.system.system[host]
            exp_file_paths = exp.system_file_paths

            kwargs = {}
            test_vars = {
                "t2m": "air_temperature_2m",
                "rh2m": "relative_humidity_2m",
                "sd": "surface_snow_thickness"
            }
            for var, var_name in test_vars.items():
                logging.debug("%s %s", var, var_name)
                task = scheduler.EcflowTask("/test/" + var + "/Task", 1, "ecf_pass", 11, None)
                output = f"{archive_dir}/an_" + var_name + ".nc"
                if os.path.exists(output):
                    os.unlink(output)
                experiment_tasks.OptimalInterpolation(task, config, system, exp_file_paths,
                                                      self.progress,
                                                      **kwargs).run()
                logging.debug("output: %s", output)
                self.assertTrue(os.path.exists(output))

    def test_oi2soda(self):
        """Test task."""
        stream = None

        test_hosts = ["0", "1"]
        for host in test_hosts:

            lib_dir = self.system.get_var("SFX_EXP_LIB", host)
            data_dir = self.system.get_var("SFX_EXP_DATA", host)

            # Prepare data
            root = f"{data_dir}/test/oi2soda/"
            os.system(f"rm -r {root}")
            out_dir = f"{root}/archive/observations/2022/04/28/06/"
            os.system(f"mkdir -p {out_dir}")
            output = f"{out_dir}/OBSERVATIONS_220428H06.DAT"

            obs_dir_pattern = f"{root}/archive/observations/@YYYY@/@MM@/@DD@/@HH@/@EEE@"
            archive_dir = f"{root}/archive/2022/04/28/06/"
            archive_dir_pattern = f"{root}/archive/@YYYY@/@MM@/@DD@/@HH@/@EEE@"
            os.system(f"mkdir -p {archive_dir}")
            os.system(f"cp -r {TESTDATA}/archive/2022/04/28/06/an_*.nc {archive_dir}/.")

            if os.path.exists(output):
                os.unlink(output)

            env_input = json.load(open(f"{lib_dir}/Env_input_paths", mode="r", encoding="utf-8"))
            env_input.update({
                "obs_dir": obs_dir_pattern,
                "archive_dir": archive_dir_pattern
            })

            system_file_paths = experiment.SystemFilePathsFromSystem(env_input, self.system,
                                                                     hosts=self.system.hosts,
                                                                     stream=stream,
                                                                     wdir=self.wdir).paths[host]

            merged_config, member_config = \
                experiment_setup.process_merged_settings(self.merged_config)
            exp = experiment.Exp(self.exp_dependencies, merged_config, member_config,
                                 submit_exceptions=None, system_file_paths=system_file_paths,
                                 system=self.system, server=None, env_submit=self.env_submit,
                                 progress=None)

            config = exp.config.settings
            system = exp.system.system[host]
            exp_file_paths = exp.system_file_paths

            kwargs = {}
            experiment_tasks.Oi2soda(self.task, config, system, exp_file_paths, self.progress,
                                     **kwargs).run()
            self.assertTrue(os.path.exists(output))

    def test_qc2obsmon(self):
        """Test task."""
        stream = None

        test_hosts = ["1"]
        for host in test_hosts:

            lib_dir = self.system.get_var("SFX_EXP_LIB", host)
            data_dir = self.system.get_var("SFX_EXP_DATA", host)

            output = f"{data_dir}/archive/extract/ecma_sfc/2022042806/ecma.db"
            if os.path.exists(output):
                os.unlink(output)

            env_input = json.load(open(f"{lib_dir}/Env_input_paths", mode="r", encoding="utf-8"))
            first_guess_dir = "/tmp/host" + host + "/testdata/archive/@YYYY@/@MM@/@DD@/@HH@/@EEE@"
            obs_dir = "/tmp/host" + host + \
                      "/testdata/archive/observations/@YYYY@/@MM@/@DD@/@HH@/@EEE@"
            env_input.update({
                "first_guess_dir": first_guess_dir,
                "archive_dir": first_guess_dir,
                "obs_dir": obs_dir
            })

            system_file_paths = experiment.SystemFilePathsFromSystem(env_input, self.system,
                                                                     hosts=self.system.hosts,
                                                                     stream=stream,
                                                                     wdir=self.wdir).paths[host]

            merged_config, member_config = \
                experiment_setup.process_merged_settings(self.merged_config)
            exp = experiment.Exp(self.exp_dependencies, merged_config, member_config,
                                 submit_exceptions=None, system_file_paths=system_file_paths,
                                 system=self.system, server=None, env_submit=self.env_submit,
                                 progress=None)

            config = exp.config.settings
            system = exp.system.system[host]
            exp_file_paths = exp.system_file_paths
            kwargs = {}
            experiment_tasks.Qc2obsmon(self.task, config, system, exp_file_paths,
                                       self.progress, **kwargs).run()
            self.assertTrue(os.path.exists(output))
