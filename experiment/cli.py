#!/usr/bin/env python3
"""Implement the package's commands."""
import argparse
import sys

from deode.commands_functions import set_deode_home
from deode.config_parser import ConfigParserDefaults, ParsedConfig

from .experiment import case_setup


def case_cli():
    """Entry point."""
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
    output_file = args.output_file
    case = args.case
    domain = args.domain
    ial_source = args.ial_source
    namelist_defs = args.namelist_defs
    binary_input_files = args.binary_input_files
    base_config_file = args.base_config_file
    case_config_file = args.case_config_file
    case_setup(
        config,
        output_file,
        case=case,
        domain=domain,
        host=host,
        ial_source=ial_source,
        namelist_defs=namelist_defs,
        binary_input_files=binary_input_files,
        config_dir=config_dir,
        base_config_file=base_config_file,
        case_config_file=case_config_file,
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
    parser.add_argument("--config-dir", help="Config dir", required=False, default=None)
    parser.add_argument("--casedir", help="Case dir", required=False, default=None)
    parser.add_argument(
        "--output",
        "-o",
        dest="output_file",
        help="Output config file",
        required=True,
        default=None,
    )
    parser.add_argument(
        "--case-name", dest="case", help="Case name", required=False, default=None
    )
    parser.add_argument("--domain", "-d", help="domain", required=False, default=None)
    parser.add_argument(
        "--source",
        "-s",
        dest="ial_source",
        help="IAL source",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--namelist",
        "-n",
        dest="namelist_defs",
        help="Namelist definitions",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--input",
        "-i",
        dest="binary_input_files",
        help="Binary input data",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--predef",
        "-p",
        dest="base_config_file",
        help="Path to pre-defined configuration",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--case-config",
        dest="case_config_file",
        help="Path to case configuration",
        required=False,
        default=None,
    )
    args = parser.parse_args()
    return args
