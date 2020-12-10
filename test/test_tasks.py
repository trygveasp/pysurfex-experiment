import surfex
import unittest
import experiment
import scheduler
import os
import datetime


class TaskTest(unittest.TestCase):

    def setUp(self):
        progress = {
            "DTG": datetime.datetime(2020, 11, 13, 6),
            "DTGEND": datetime.datetime(2020, 11, 13, 6),
            "DTGBEG": datetime.datetime(2020, 11, 13, 3)
        }
        progress_pp = {"DTGPP": datetime.datetime(2020, 11, 13, 6)}
        self.progress = experiment.Progress(progress, progress_pp)
        self.stream = None
        self.mbr = None
        self.host = "1"
        self.wrapper = ""
        self.ecf_tryno = 1
        self.ecf_pass = "abcef"
        self.ecf_rid = ""
        self.submission_id = ""
        self.args = None

    @staticmethod
    def _create_exp(wd, rev):
        os.makedirs(wd, exist_ok=True)
        argv = [
            "setup",
            "--wd", wd,
            "-surfex", surfex.__path__[0],
            "-scheduler", scheduler.__path__[0],
            "-rev", rev,
            "-host", "unittest",
            "--debug",
            "--domain_file", "test/settings/conf_proj_test.json"
        ]
        kwargs = experiment.parse_surfex_script(argv)
        experiment.surfex_script(**kwargs)

    def test_pgd(self):
        exp_name = "pgd"
        root = os.getcwd()
        wd = "/tmp/host0/hm_wd/" + exp_name
        self._create_exp(wd, root)
        exp = experiment.ExpFromFiles(exp_name, wd, debug=True)
        experiment.init_run(exp, self.stream)
        wd = "/tmp/host1/scratch/sfx_home/" + exp_name + "/lib"

        exp = experiment.ExpFromFiles(exp_name, wd, host=self.host, stream=self.stream, progress=self.progress)

        ecf_name = "/" + exp_name + "/task"
        task = scheduler.EcflowTask(ecf_name, self.ecf_tryno, self.ecf_pass, self.ecf_rid, self.submission_id)
        experiment.Pgd(task, exp, host=self.host, mbr=self.mbr, stream=self.stream,
                       args=self.args).run(wrapper=self.wrapper)
        os.chdir(root)

    def test_prep(self):
        exp_name = "prep_task"
        root = os.getcwd()
        wd = "/tmp/host0/hm_wd/" + exp_name
        self._create_exp(wd, root)
        exp = experiment.ExpFromFiles(exp_name, wd)
        experiment.init_run(exp, self.stream)
        wd = "/tmp/host1/scratch/sfx_home/" + exp_name + "/lib"

        exp = experiment.ExpFromFiles(exp_name, wd, host=self.host, stream=self.stream, progress=self.progress)
        ecf_name = "/" + exp_name + "/task"
        task = scheduler.EcflowTask(ecf_name, self.ecf_tryno, self.ecf_pass, self.ecf_rid, self.submission_id)
        experiment.Prep(task, exp, host=self.host, mbr=self.mbr, stream=self.stream,
                        args=self.args).run(wrapper=self.wrapper)
        os.chdir(root)

    def test_quality_control_t2m(self):
        exp_name = "quality_control_t2m_task"
        root = os.getcwd()
        wd = "/tmp/host0/hm_wd/" + exp_name
        self._create_exp(wd, root)
        exp = experiment.ExpFromFiles(exp_name, wd)
        experiment.init_run(exp, self.stream)
        wd = "/tmp/host1/scratch/sfx_home/" + exp_name + "/lib"

        exp = experiment.ExpFromFiles(exp_name, wd, host=self.host, stream=self.stream, progress=self.progress)
        ecf_name = "/" + exp_name + "/task"
        task = scheduler.EcflowTask(ecf_name, self.ecf_tryno, self.ecf_pass, self.ecf_rid, self.submission_id)
        task.family1 = "t2m"
        experiment.QualityControl(task, exp, host=self.host, mbr=self.mbr, stream=self.stream,
                                  args=self.args).run(wrapper=self.wrapper)
        os.chdir(root)

    def test_quality_control_rh2m(self):
        exp_name = "quality_control_rh2m_task"
        root = os.getcwd()
        wd = "/tmp/host0/hm_wd/" + exp_name
        self._create_exp(wd, root)
        exp = experiment.ExpFromFiles(exp_name, wd,)
        experiment.init_run(exp, self.stream)
        wd = "/tmp/host1/scratch/sfx_home/" + exp_name + "/lib"

        exp = experiment.ExpFromFiles(exp_name, wd, host=self.host, stream=self.stream, progress=self.progress)
        ecf_name = "/" + exp_name + "/task"
        task = scheduler.EcflowTask(ecf_name, self.ecf_tryno, self.ecf_pass, self.ecf_rid, self.submission_id)
        task.family1 = "rh2m"
        experiment.QualityControl(task, exp, host=self.host, mbr=self.mbr, stream=self.stream,
                                  args=self.args).run(wrapper=self.wrapper)
        os.chdir(root)

    def test_quality_control_sd(self):
        exp_name = "quality_control_sd_task"
        root = os.getcwd()
        wd = "/tmp/host0/hm_wd/" + exp_name
        self._create_exp(wd, root)
        exp = experiment.ExpFromFiles(exp_name, wd,)
        experiment.init_run(exp, self.stream)
        wd = "/tmp/host1/scratch/sfx_home/" + exp_name + "/lib"

        exp = experiment.ExpFromFiles(exp_name, wd, host=self.host, stream=self.stream, progress=self.progress)
        ecf_name = "/" + exp_name + "/task"
        task = scheduler.EcflowTask(ecf_name, self.ecf_tryno, self.ecf_pass, self.ecf_rid, self.submission_id)
        task.family1 = "sd"
        experiment.QualityControl(task, exp, host=self.host, mbr=self.mbr, stream=self.stream,
                                  args=self.args).run(wrapper=self.wrapper)
        os.chdir(root)
