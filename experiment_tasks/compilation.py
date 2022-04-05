from experiment_tasks import AbstractTask
import surfex
import os


class ConfigureOfflineBinaries(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)
        self.var_name = task.family1

    def execute(self, **kwargs):

        rte = os.environ
        wrapper = ""
        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib")
        flavour = self.system["SURFEX_CONFIG"]
        surfex.BatchJob(rte, wrapper=wrapper).run("export HARMONIE_CONFIG=" + flavour + " && cd " + sfx_lib +
                                                  "/offline/src && ./configure OfflineNWP ../conf//system." + flavour)


class MakeOfflineBinaries(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)
        self.var_name = task.family1

    def execute(self, **kwargs):

        rte = os.environ
        wrapper = ""
        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib")
        flavour = self.system["SURFEX_CONFIG"]
        xyz = self.config.get_setting("COMPILE#XYZ")

        system_file = sfx_lib + "/offline/conf/system." + flavour
        conf_file = sfx_lib + "/offline/conf/profile_surfex-" + flavour

        xyz_file = self.wdir + "/xyz"
        cmd = "source " + conf_file + "; echo $XYZ > " + xyz_file
        print(cmd)
        try:
            os.system(cmd)
        except Exception as ex:
            raise Exception("Can write XYZ " + str(ex))

        xyz2 = open(xyz_file, "r").read().rstrip()
        if xyz2 != xyz:
            raise Exception("Mismatch betweeen XYZ in config files! :" + xyz + ": != :" + xyz2 + ":")

        surfex.BatchJob(rte, wrapper=wrapper).run("source " + system_file + "; source " + conf_file + 
                                                  "; cd " + sfx_lib + "/offline/src && make -j 16")
        surfex.BatchJob(rte, wrapper=wrapper).run("source " + system_file + "; source " + conf_file +
                                                  "; cd " + sfx_lib + "/offline/src && make installmaster")
