from experiment_tasks import AbstractTask
import surfex
import os


class ConfigureOfflineBinaries(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress,**kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)

    def execute(self):

        rte = os.environ
        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib")
        flavour = self.system["SURFEX_CONFIG"]
        surfex.BatchJob(rte, wrapper=self.wrapper).run("export HARMONIE_CONFIG=" + flavour + " && cd " + sfx_lib +
                                                       "/offline/src && ./configure OfflineNWP ../conf//system." +
                                                       flavour)

        conf_file = sfx_lib + "/offline/conf/profile_surfex-" + flavour
        xyz_file = sfx_lib + "/xyz"
        cmd = ". " + conf_file + "; echo \"$XYZ\" > " + xyz_file
        print(cmd)
        try:
            os.system(cmd)
        except Exception as ex:
            raise Exception("Can not write XYZ " + str(ex))


class MakeOfflineBinaries(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)

    def execute(self):

        rte = os.environ
        wrapper = ""
        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib")
        flavour = self.system["SURFEX_CONFIG"]

        system_file = sfx_lib + "/offline/conf/system." + flavour
        conf_file = sfx_lib + "/offline/conf/profile_surfex-" + flavour

        surfex.BatchJob(rte, wrapper=wrapper).run(". " + system_file + "; . " + conf_file + 
                                                  "; cd " + sfx_lib + "/offline/src && make -j 4")
        surfex.BatchJob(rte, wrapper=wrapper).run(". " + system_file + "; . " + conf_file +
                                                  "; cd " + sfx_lib + "/offline/src && make installmaster")
