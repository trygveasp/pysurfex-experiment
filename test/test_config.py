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
        cls.sfx_exp = experiment.ExpFromFiles(cls.exp_dependencies, stream=stream)

    def test_check_experiment_path(self):
        """Test if exp_dependencies contain some expected variables."""
        str1 = self.exp_dependencies["pysurfex_experiment"]
        str2 = self.pysurfex_experiment
        text = "pysurfex_experiment string mismatch"
        self.assertEqual(str1, str2, text)

    def test_read_setting(self):
        """Read normal settings."""
        logging.debug("Read setting")
        build = self.sfx_exp.get_setting("COMPILE#BUILD")
        print(build)
        self.assertFalse(build, "Build is true")

    def test_read_member_setting(self):
        """Read member settings."""
        logging.debug("Read member setting")
        sfx_exp = experiment.ExpFromFiles(self.exp_dependencies)
        sfx_exp.update_setting("GENERAL#ENSMBR", 2)
        build = sfx_exp.get_setting("COMPILE#BUILD")
        self.assertFalse(build, "Build is true")

    def test_update_setting(self):
        """Update setting."""
        sfx_exp = experiment.ExpFromFiles(self.exp_dependencies)
        sfx_exp.update_setting("GENERAL#ENSMBR", 2)
