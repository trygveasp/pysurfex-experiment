"""Experiment module."""
__version__ = "0.0.1a5"

from .suites import get_defs, SurfexSuite
from .cli import parse_surfex_script, surfex_script, parse_update_config, update_config, surfex_exp, \
                 surfex_exp_config
from .configuration import Configuration, ConfigurationFromJsonFile
from .experiment import Exp, ExpFromFiles, ExpFromFilesDepFile
from .progress import Progress, ProgressFromFiles, ProgressFromDict
from .system import System, SystemFilePathsFromSystem, SystemFilePathsFromSystemFile, SystemFromFile

__all__ = ["get_defs", "SurfexSuite", "surfex_exp", "surfex_exp_config",
           "parse_surfex_script", "surfex_script", "parse_update_config", "update_config",
           "Exp", "ExpFromFiles", "ExpFromFilesDepFile",
           "Configuration", "ConfigurationFromJsonFile",
           "Progress", "ProgressFromFiles", "ProgressFromDict",
           "System", "SystemFilePathsFromSystem", "SystemFilePathsFromSystemFile", "SystemFromFile"
           ]
