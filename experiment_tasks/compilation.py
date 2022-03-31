from experiment_tasks import AbstractTask
import surfex
import os
import json


class ConfigureOfflineBinaries(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)
        self.var_name = task.family1

    def execute(self, **kwargs):

        rte = os.environ
        wrapper = ""
        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib")
        flavour = "ppi_centos7"
        surfex.BatchJob(rte, wrapper=wrapper).run("export HARMONIE_CONFIG=" + flavour + " && cd " + sfx_lib +
                                                  "/offline/src && ./configure OfflineNWP ../conf//system." + flavour)


class MakeOfflineBinaries(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)
        self.var_name = task.family1

    def execute(self, **kwargs):

        print("TRYGVE0", os.environ)
        rte = os.environ
        wrapper = ""
        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib")
        # xyz = self.config.get_setting("COMPILE#XYZ")

        flavour = "ppi_centos7"
        cmd = "source /modules/centos7/conda/Feb2021/etc/profile.d/conda.sh; conda activate production-04-2021; env | grep -i conda; source " \
              "" + sfx_lib + "/offline/conf/system." + flavour + \
              "; /modules/centos7/user-apps/suv/pysurfex/0.0.1-dev/bin/dump_environ -o " + self.wdir + "/rte.json"
        try:
            os.system(cmd)
        except Exception as ex:
            raise Exception("Failed with exception " + str(ex))

        print("TRYGVE1", os.environ)

        rte = json.load(open(self.wdir + "/rte.json", "r"))

        surfex.BatchJob(rte, wrapper=wrapper).run("source " + sfx_lib + "/offline/conf/profile_surfex-" + flavour +
                                                  "; cd " + sfx_lib + "/offline/src && make -j 16")
        surfex.BatchJob(rte, wrapper=wrapper).run("source " + sfx_lib + "/offline/conf/profile_surfex-" + flavour +
                                                  "; cd " + sfx_lib + "/offline/src && make installmaster")
