"""Unit testing."""
# import os
# import sys
import unittest
from pathlib import Path
# import json
# import inspect
# import shutil
import logging
# import toml
# import experiment_scheduler as scheduler
from unittest.mock import patch
import experiment
import surfex
# import experiment_setup
# import experiment_tasks
# import ecf

TESTDATA = f"{str((Path(__file__).parent).parent)}/testdata"
ROOT = f"{str((Path(__file__).parent).parent)}"
logging.basicConfig(format='%(asctime)s %(levelname)s %(pathname)s:%(lineno)s %(message)s',
                    level=logging.DEBUG)


class TestConfig(unittest.TestCase):
    """Test config."""

    @classmethod
    def setUpClass(cls):
        wdir = "/tmp/test_config"
        exp_name = "test_config"
        host = "unittest"
        cls.pysurfex_experiment = f"{str((Path(__file__).parent).parent)}"
        pysurfex = f"{str((Path(surfex.__file__).parent).parent)}"
        offline_source = "/tmp/source"

        cls.exp_dependencies = experiment.ExpFromFiles.setup_files(wdir, exp_name, host, pysurfex,
                                                                   cls.pysurfex_experiment,
                                                                   offline_source=offline_source)
        stream = None
        with patch('experiment_scheduler.scheduler.ecflow') as mock_ecflow:
            cls.sfx_exp = experiment.ExpFromFiles(cls.exp_dependencies, stream=stream)
            cls.sfx_exp.update_setting("COMPILE#TEST_TRUE", True)
            cls.sfx_exp.update_setting("COMPILE#TEST_VALUES", [1, 2, 4])
            cls.sfx_exp.update_setting("COMPILE#TEST_SETTING", "SETTING")


    def test_check_experiment_path(self):
        """Test if exp_dependencies contain some expected variables."""
        str1 = self.exp_dependencies["pysurfex_experiment"]
        str2 = self.pysurfex_experiment
        text = "pysurfex_experiment string mismatch"
        self.assertEqual(str1, str2, text)

    def test_read_setting(self):
        """Read normal settings."""
        logging.debug("Read setting")
        build = self.sfx_exp.get_setting("COMPILE#TEST_TRUE")
        print(build)
        self.assertTrue(build, "Build is false")

    @patch('experiment_scheduler.scheduler.ecflow')
    def test_read_member_setting(self, mock_ecflow):
        """Read member settings."""
        logging.debug("Read member setting")
        sfx_exp = experiment.ExpFromFiles(self.exp_dependencies)
        sfx_exp.update_setting("GENERAL#ENSMBR", 2)
        self.assertEqual(sfx_exp.get_setting("GENERAL#ENSMBR"), 2, "Member not 2")

    @patch('experiment_scheduler.scheduler.ecflow')
    def test_update_setting(self, mock_ecflow):
        """Update setting."""
        sfx_exp = experiment.ExpFromFiles(self.exp_dependencies)
        sfx_exp.update_setting("GENERAL#ENSMBR", 2)
        self.assertEqual(sfx_exp.get_setting("GENERAL#ENSMBR"), 2, "Member not 2")

    def test_dump_json(self):
        self.sfx_exp.dump_json("/tmp/dump_json.json", indent=2)

    def test_max_fc_length(self):
        self.sfx_exp.max_fc_length()

    def test_setting_is_not(self):
        self.assertTrue(self.sfx_exp.setting_is_not("COMPILE#TEST_TRUE", False))

    def test_setting_is_not_one_of(self):
        self.assertTrue(self.sfx_exp.setting_is_not_one_of("COMPILE#TEST_SETTING",
                        ["NOT_A_SETTING"]), "Settings does exist")

    def test_setting_is_one_of(self):
        self.assertTrue(self.sfx_exp.setting_is_one_of("COMPILE#TEST_SETTING",
                        ["SETTING", "NOT_A_SETTING"]), "Both setting do not exist")

    def test_value_is_not_one_of(self):
        self.assertTrue(self.sfx_exp.value_is_not_one_of("COMPILE#TEST_VALUES", 3), "Settings has value 3")

    def test_value_is_one_of(self):
        self.assertTrue(self.sfx_exp.value_is_one_of("COMPILE#TEST_VALUES", 1), "Setting has not value 1")

    def test_write_exp_config(self):
        experiment.ExpFromFiles.write_exp_config(self.exp_dependencies, configuration="sekf", configuration_file=None)
