from experiment_tasks import AbstractTask
import os
import time


# Two test cases
class UnitTest(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)

    def execute(self):
        os.makedirs("/tmp/host0/job/test_start_and_run/", exist_ok=True)
        fh = open("/tmp/host1/scratch/sfx_home/test_start_and_run/unittest_ok", "w")
        fh.write("ok")
        fh.close()


class SleepingBeauty(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)

    def execute(self):
        print("Sleeping beauty...")
        print("Create /tmp/host1/scratch/sfx_home/test_start_and_run/SleepingBeauty")
        os.makedirs("/tmp/host0/job/test_start_and_run/", exist_ok=True)
        fh = open("/tmp/host1/scratch/sfx_home/test_start_and_run/SleepingBeauty", "w")
        fh.write("SleepingBeauty")
        fh.close()
        for i in range(0, 20):
            print("sleep.... ", i, "\n")
            time.sleep(1)


class SleepingBeauty2(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)

    def execute(self):
        print("Will the real Sleeping Beauty, please wake up! please wake up!")
        print("Create /tmp/host1/scratch/sfx_home/test_start_and_run/SleepingBeauty2")
        os.makedirs("/tmp/host0/job/test_start_and_run/", exist_ok=True)
        fh = open("/tmp/host1/scratch/sfx_home/test_start_and_run/SleepingBeauty2", "w")
        fh.write("SleepingBeauty")
        fh.close()


class WakeUpCall(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)

    def execute(self):
        print("This job is default suspended and manually submitted!")
        print("Create /tmp/host1/scratch/sfx_home/test_start_and_run/test_submit")
        os.makedirs("/tmp/host0/job/test_start_and_run/", exist_ok=True)
        fh = open("/tmp/host1/scratch/sfx_home/test_start_and_run/test_submit", "w")
        fh.write("Job was submitted")
        fh.close()
