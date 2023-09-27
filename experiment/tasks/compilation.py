"""Compilation."""
import os
import shutil

from pysurfex.run import BatchJob

from ..logs import logger
from ..tasks.tasks import AbstractTask


class SyncSourceCode(AbstractTask):
    """Sync source code for offline code.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct SyncSourceCode task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        AbstractTask.__init__(self, config, "SyncSourceCode")

    def execute(self):
        """Execute."""
        rte = os.environ
        wrapper = ""

        rsync = self.platform.get_system_value("rsync")
        sfx_lib = f"{self.platform.get_system_value('sfx_exp_lib')}/offline"
        os.makedirs(sfx_lib, exist_ok=True)
        offline_source = self.config.get_value("compile.offline_source")
        ifsaux = f"{offline_source}/../../src/ifsaux"
        ifsaux_copy = f"{offline_source}/src/LIB/ifsaux_copy"
        if os.path.exists(ifsaux):
            cmd = f"{rsync} {ifsaux}/* {ifsaux_copy}"
            logger.info(cmd)
            BatchJob(rte, wrapper=wrapper).run(cmd)
        cmd = f"{rsync} {offline_source}/ {sfx_lib}"
        logger.info(cmd)
        BatchJob(rte, wrapper=wrapper).run(cmd)

        # Add system files if not existing
        scripts = self.platform.get_system_value("pysurfex_experiment")
        host = self.platform.get_system_value("surfex_config")

        system_file_scripts = f"{scripts}/config/offline/conf/system.{host}"
        system_file_lib = f"{sfx_lib}/conf/system.{host}"

        logger.debug("Check system_file_lib {}", system_file_lib)
        if not os.path.exists(system_file_lib):
            logger.debug("Check system_file_scripts {}", system_file_scripts)
            if os.path.exists(system_file_scripts):
                logger.info("Copy {} {}", system_file_scripts, system_file_lib)
                shutil.copy(system_file_scripts, system_file_lib)

        rules_file_scripts = f"{scripts}/config/offline/src/Rules.{host}.mk"
        rules_file_lib = f"{sfx_lib}/src/Rules.{host}.mk"

        logger.debug("Check rules_file_lib {}", rules_file_lib)
        if not os.path.exists(rules_file_lib):
            logger.debug("Check rules_file_scripts {}", rules_file_scripts)
            if os.path.exists(rules_file_scripts):
                logger.info("Copy {} {}", rules_file_scripts, rules_file_lib)
                shutil.copy(rules_file_scripts, rules_file_lib)


class ConfigureOfflineBinaries(AbstractTask):
    """Configure offline binaries.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct ConfigureOfflineBinaries task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        AbstractTask.__init__(self, config, "ConfigureOfflineBinaries")

    def execute(self):
        """Execute."""
        rte = os.environ
        sfx_lib = f"{self.platform.get_system_value('sfx_exp_lib')}"
        flavour = self.surfex_config
        cmd = (
            f"export OFFLINE_CONFIG={flavour} && cd {sfx_lib}/offline/src && "
            f"./configure OfflineNWP ../conf//system.{flavour}"
        )
        logger.debug(cmd)
        BatchJob(rte, wrapper=self.wrapper).run(cmd)

        conf_file = sfx_lib + "/offline/conf/profile_surfex-" + flavour
        xyz_file = sfx_lib + "/xyz"
        cmd = ". " + conf_file + '; echo "$XYZ" > ' + xyz_file
        logger.info(cmd)
        try:
            os.system(cmd)  # noqa
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
            config (ParsedObject): Parsed configuration

        """
        AbstractTask.__init__(self, config, "MakeOfflineBinaries")

    def execute(self):
        """Execute."""
        rte = {**os.environ}
        wrapper = ""
        sfx_lib = f"{self.platform.get_system_value('sfx_exp_lib')}"
        flavour = self.surfex_config

        system_file = sfx_lib + "/offline/conf/system." + flavour
        conf_file = sfx_lib + "/offline/conf/profile_surfex-" + flavour

        cmd = f". {system_file}; . {conf_file}; cd {sfx_lib}/offline/src && make -j 4"
        logger.debug(cmd)
        BatchJob(rte, wrapper=wrapper).run(cmd)

        os.makedirs(f"{sfx_lib}/offline/exe", exist_ok=True)
        cmd = f". {system_file}; . {conf_file}; cd {sfx_lib}/offline/src && make installmaster"
        logger.debug(cmd)
        BatchJob(rte, wrapper=wrapper).run(cmd)


class CMakeBuild(AbstractTask):
    """Make offline binaries.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, config):
        """Construct CMakeBuild task.

        Args:
            config (ParsedObject): Parsed configuration

        """
        AbstractTask.__init__(self, config, "CMakeBuild")

    def execute(self):
        """Execute."""
        rte = {**os.environ}
        wrapper = ""

        nproc = 8
        offline_source = self.config.get_value("compile.offline_source")
        cmake_config = self.platform.get_system_value("surfex_config")
        cmake_config = f"{offline_source}/util/cmake/config/config.{cmake_config}.json"
        if not os.path:
            raise FileNotFoundError(f"CMake config file {cmake_config} not found!")
        sfx_lib = f"{self.platform.get_system_value('sfx_exp_lib')}"
        build_dir = f"{sfx_lib}/offline/"
        install_dir = f"{sfx_lib}/offline/exe"
        os.makedirs(install_dir, exist_ok=True)
        prerequisites = ["gribex_370"]
        for project in prerequisites:
            logger.info("Compiling {}", project)
            current_project_dir = f"{offline_source}/util/auxlibs/{project}"
            fproject = project.replace("/", "-")
            current_build_dir = f"{build_dir}/{fproject}"
            os.makedirs(current_build_dir, exist_ok=True)
            os.chdir(current_build_dir)
            cmake_flags = "-DCMAKE_BUILD_TYPE=Release "
            cmake_flags += (
                f"-DCMAKE_INSTALL_PREFIX={install_dir} -DCONFIG_FILE={cmake_config}"
            )
            cmd = f"cmake {current_project_dir} {cmake_flags}"
            BatchJob(rte, wrapper=wrapper).run(cmd)
            cmd = f"cmake --build . -j{nproc} --target gribex"
            BatchJob(rte, wrapper=wrapper).run(cmd)
            cmd = "cmake --build . --target install"
            BatchJob(rte, wrapper=wrapper).run(cmd)

        cmake_flags = "-DCMAKE_BUILD_TYPE=Release "
        cmake_flags += f"{cmake_flags} -DCMAKE_INSTALL_PREFIX={install_dir}"
        cmake_flags += f"{cmake_flags} -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=YES"
        cmake_flags += f"{cmake_flags} -DCONFIG_FILE={cmake_config}"
        os.makedirs(build_dir, exist_ok=True)
        os.chdir(build_dir)

        # Configure
        cmd = f"cmake {offline_source}/src {cmake_flags}"
        BatchJob(rte, wrapper=wrapper).run(cmd)
        # Build
        cmd = f"cmake --build . -- -j{nproc} offline-pgd offline-prep offline-offline offline-soda"
        BatchJob(rte, wrapper=wrapper).run(cmd)

        # Manual installation
        programs = ["PGD-offline", "PREP-offline", "OFFLINE-offline", "SODA-offline"]
        for program in programs:
            logger.info("Installing {}", program)
            shutil.copy(f"{build_dir}/bin/{program}", f"{install_dir}/{program}")

        xyz_file = sfx_lib + "/xyz"
        with open(xyz_file, mode="w", encoding="utf-8") as fhandler:
            fhandler.write("-offline")
