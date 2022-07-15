"""Unit testing."""
import unittest
from pathlib import Path
import logging
import experiment_scheduler
import experiment_setup
import experiment
import scheduler

THIS_DIR = Path(__file__).parent
MY_DATA_PATH = str(THIS_DIR.parent)


TESTDATA = f"{str((Path(__file__).parent).parent)}/testdata"
logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                    level=logging.DEBUG)

class TestEcflowServerClients(unittest.TestCase):
    """Test the client programs to submit, check and kill jobs."""

    @classmethod
    def setUpClass(cls):
        """Set up.

        Create an experiment with a scheduler.json file to use for testing.

        """
        pysurfex = f"{MY_DATA_PATH}/../pysurfex/"
        config_files = experiment.ExpFromFiles.get_config_files_dict(MY_DATA_PATH,
                                                                     pysurfex=pysurfex,
                                                                     must_exists=True)
        cls.default_config = experiment_setup.merge_toml_env_from_config_dicts(config_files)

        exp_name = "TestEcfClient"
        cls.settings_file = "/tmp/scheduler.json"
        exp_dependencies = {
            "revision": "",
            "pysurfex_experiment": "",
            "pysurfex": "",
            "offline_source": "",
            "exp_dir": "/tmp/",
            "exp_name": exp_name
        }
        ecf_host = "localhost"
        ecf_port = "1234"
        logfile = "/dev/null"
        server = scheduler.EcflowServer(ecf_host, ecf_port, logfile)
        env_system = {
            "HOST_SYSTEM": {
                "COMPCENTRE": "LOCAL",
                "HOSTS": ["my_host_0"],
                "SCHEDULER_PYTHONPATH": "",
                "SFX_EXP_DATA": "",
                "SFX_EXP_LIB": "",
                "JOBOUTDIR": "",
                "MKDIR": "mkdir -p",
                "RSYNC": "",
                "LOGIN_HOST": "",
                "SURFEX_CONFIG": ""
            }
        }
        system = experiment.System(env_system, exp_name)
        env_submit = {
            "submit_types": ["background"],
            "default_submit_type": "background",
            "background": {
                "HOST": "0",
            }
        }
        submit_exceptions = {}
        exp = experiment.Exp(exp_dependencies, cls.default_config, {}, env_submit=env_submit,
                             server=server, system=system,
                             submit_exceptions=submit_exceptions)
        exp.write_scheduler_info("ECF.log", filename=cls.settings_file)

    def test_ecf_submit(self):
        """Test ecf_submit."""
        argv = [
            "-exp", self.settings_file,
            "-ecf_name", "/Test/Test_task",
            "--debug"
        ]
        kwargs = experiment_scheduler.parse_submit_cmd_exp(argv)
        experiment_scheduler.submit_cmd_exp(**kwargs)

    def test_ecf_status(self):
        """Test ecf_status."""
        argv = [
            "-exp", self.settings_file,
            "-ecf_name", "/Test/Test_task",
            "-ecf_tryno", "1",
            "-ecf_pass", "1234",
            "--debug"
        ]
        kwargs = experiment_scheduler.parse_status_cmd_exp(argv)
        experiment_scheduler.status_cmd_exp(**kwargs)

    def test_ecf_kill(self):
        """Test ecf_kill."""
        argv = [
            "-exp", self.settings_file,
            "-ecf_name", "/Test/Test_task",
            "-ecf_tryno", "1",
            "-ecf_pass", "1234",
            "--debug"
        ]
        kwargs = experiment_scheduler.parse_kill_cmd_exp(argv)
        experiment_scheduler.kill_cmd_exp(**kwargs)
