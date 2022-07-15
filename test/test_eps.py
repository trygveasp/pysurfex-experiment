"""To be removed."""
import unittest
from pathlib import Path
import logging
import experiment_scheduler
import experiment_setup
import experiment
import scheduler
import json

THIS_DIR = Path(__file__).parent
MY_DATA_PATH = str(THIS_DIR.parent)

TESTDATA = f"{str((Path(__file__).parent).parent)}/testdata"
logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                    level=logging.DEBUG)


class TestEPS(unittest.TestCase):
    """Test EPS."""

    @classmethod
    def setUpClass(cls):
        """Set up.

        Create an experiment with a scheduler.json file to use for testing.

        """
        pysurfex = f"{MY_DATA_PATH}/../pysurfex/"
        pysurfex_experiment = f"{MY_DATA_PATH}/"
        config_files = experiment.ExpFromFiles.get_config_files_dict(MY_DATA_PATH,
                                                                     pysurfex=pysurfex,
                                                                     must_exists=True)
        cls.default_config = experiment_setup.merge_toml_env_from_config_dicts(config_files)

        cls.exp_name = "TestEPS"
        cls.exp_dependencies = {
            "revision": "",
            "pysurfex_experiment": pysurfex_experiment,
            "pysurfex": pysurfex,
            "offline_source": None,
            "exp_dir": f"/tmp/{cls.exp_name}",
            "exp_name": cls.exp_name
        }
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
        cls.system = experiment.System(env_system, cls.exp_name)
        cls.env_submit = {
            "submit_types": ["background"],
            "default_submit_type": "background",
            "background": {
                "HOST": "0",
            }
        }
        cls.submit_exceptions = {}

        cls.eps_settings = {
            "FORECAST": {
                "ENSMSEL": ["0", "1", "2"]
            },
            "EPS": {
                "MEMBER_SETTINGS": {
                    "GENERAL": {
                        "HH_LIST": {
                            "0": '0-21:3',
                            "1": '0-23:1',
                            "2": '0-18:6'
                        },
                        "LL_LIST": {
                            "0": '3',
                            "1": '3',
                            "2": '3'
                        }
                    },
                    "SURFEX": {
                        "ASSIM.SCHEMES": {
                            "ISBA": {
                                "0": "OI",
                                "1": "OI",
                                "2": "EKF"
                            }
                        },
                        "SEA": {
                            "PERTFLUX": {
                                "0": "none"
                            }
                        },
                        "ISBA": {
                            "PERTSURF": {
                                "0": "none"
                            }
                        }
                    }
                }
            }
        }
        cls.progress = {
            "DTG": "2022042806",
            "DTGBEG": "2022042803"
        }
        cls.task = scheduler.EcflowTask("/test/Task", 1, "ecf_pass", 11, None)

    def test_eps(self):
        merged_config = experiment_setup.merge_toml_env(self.default_config, self.eps_settings)
        merged_config, member_config = experiment_setup.process_merged_settings(merged_config)
        self.progress = {
            "DTG": "2022042806",
            "DTGBEG": "2022042803"
        }
        self.task = scheduler.EcflowTask("/test/Task", 1, "ecf_pass", 11, None)

        exp = experiment.Exp(self.exp_dependencies, merged_config, member_config,
                             env_submit=self.env_submit,
                             system=self.system,
                             submit_exceptions=self.submit_exceptions)
        hh_list = exp.config.get_cycle_list(mbr=2)
        self.assertEqual(hh_list, ['00', '06', '12', '18'])
        logging.debug("%s", exp.member_config)
        with open("/tmp/config.json", mode="w", encoding="utf-8") as file_handler:
            json.dump(exp.config.settings, file_handler, indent=2)
        with open("/tmp/member.json", mode="w", encoding="utf-8") as file_handler:
            json.dump(exp.config.member_settings, file_handler, indent=2)
        exp.config.dump_json("/tmp/sfx_exp.json")
        with open("/tmp/member.json", mode="r", encoding="utf-8") as file_handler:
            config = json.load(file_handler)

        self.assertEqual(config["2"]["GENERAL"]["FCINT"]["0000"], 21600)
