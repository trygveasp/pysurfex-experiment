#!/usr/bin/env python3
"""Implement the package's commands."""
import argparse
import os
import sys

from deode import GeneralConstants
from deode.commands_functions import set_deode_home
from deode.config_parser import ConfigParserDefaults, ParsedConfig
from deode.host_actions import DeodeHost
from deode.logs import logger

from .experiment import case_setup


def case_cli():
    """Entry point."""
    logger.enable(GeneralConstants.PACKAGE_NAME)
    args = parse_args()
    config = ParsedConfig.from_file(
        args.config_file, json_schema=ConfigParserDefaults.MAIN_CONFIG_JSON_SCHEMA
    )
    create_exp(args, config)


def create_exp(args, config):
    """Implement the 'case' command.

    Args:
        args (argparse.Namespace): Parsed command line arguments.
        config (.config_parser.ParsedConfig): Parsed config file contents.

    """
    deode_home = set_deode_home(args, config)
    config = config.copy(update={"platform": {"deode_home": deode_home}})
    config_dir = args.config_dir
    host = args.host
    if host is None:
        if config_dir is None:
            known_hosts = "data/config/known_hosts.yml"
        else:
            known_hosts = f"{config_dir}/known_hosts.yml"
    else:
        known_hosts = host
    host = DeodeHost(known_hosts=known_hosts)
    host_name = host.detect_deode_host()
    logger.info("Setting up for host {}", host_name)
    output_file = args.output_file
    case = args.case
    mod_files = args.config_mods
    if mod_files is None:
        mod_files = []
    case_setup(
        config,
        output_file,
        mod_files,
        case=case,
        host=host,
        config_dir=config_dir,
    )


def parse_args(argv=None):
    """Parse command line args.

    Args:
        argv (_type_, optional): _description_. Defaults to None.

    Returns:
        args

    """
    if argv is None:
        argv = sys.argv[1:]

    cwd = os.getcwd()
    ##########################################
    # Configure parser for the "case" command #
    ##########################################
    parser = argparse.ArgumentParser("Create a config file to run an experiment case")
    parser.add_argument(
        "--deode-home",
        default=None,
        help="Specify deode_home to override automatic detection",
    )

    parser.add_argument("--config-file", help="Config", required=True)
    parser.add_argument("--host", help="Host", required=False, default=None)
    parser.add_argument(
        "--config-dir", help="Config dir", required=False, default=f"{cwd}/data/config"
    )
    parser.add_argument(
        "--output",
        "-o",
        dest="output_file",
        help="Output config file",
        required=True,
    )
    parser.add_argument(
        "--case-name", dest="case", help="Case name", required=False, default=None
    )
    parser.add_argument(
        "config_mods",
        help="Path to configuration modifications",
        nargs="*",
        default=None,
    )
    args = parser.parse_args()
    return args
