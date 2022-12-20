"""Compilation."""
import os
import logging
import surfex
from experiment_tasks import AbstractTask


class ConfigureOfflineBinaries(AbstractTask):
    """Configure offline binaries.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct ConfigureOfflineBinaries task.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_

        """
        AbstractTask.__init__(self, config)

    def execute(self):
        """Execute."""
        rte = os.environ
        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib")
        flavour = self.surfex_config
        cmd = f"export OFFLINE_CONFIG={flavour} && cd {sfx_lib}/offline/src && " \
              f"./configure OfflineNWP ../conf//system.{flavour}"
        logging.debug(cmd)
        surfex.BatchJob(rte, wrapper=self.wrapper).run(cmd)

        conf_file = sfx_lib + "/offline/conf/profile_surfex-" + flavour
        xyz_file = sfx_lib + "/xyz"
        cmd = ". " + conf_file + "; echo \"$XYZ\" > " + xyz_file
        logging.info(cmd)
        try:
            os.system(cmd)
        except Exception as ex:
            raise Exception("Can not write XYZ ") from ex


class MakeOfflineBinaries(AbstractTask):
    """Make offline binaries.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct MakeOfflineBinaries task.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_

        """
        AbstractTask.__init__(self, config)

    def execute(self):
        """Execute."""

        for key, value in os.environ.items():
            print(f"in task0 : {key}={value}")
        rte = {**os.environ}
        wrapper = ""
        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib")
        flavour = self.surfex_config

        system_file = sfx_lib + "/offline/conf/system." + flavour
        conf_file = sfx_lib + "/offline/conf/profile_surfex-" + flavour

        cmd = f". {system_file}; . {conf_file}; cd {sfx_lib}/offline/src && make -j 4"
        logging.debug(cmd)
        surfex.BatchJob(rte, wrapper=wrapper).run(cmd)

        cmd = f". {system_file}; . {conf_file}; cd {sfx_lib}/offline/src && make installmaster"
        logging.debug(cmd)
        surfex.BatchJob(rte, wrapper=wrapper).run(cmd)
