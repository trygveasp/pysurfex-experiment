
import os
import sys
import surfex
import unittest
import experiment
import experiment_setup
import experiment_tasks
import json
import types
import importlib.machinery
import inspect
import shutil


class TestEcflowContainer(unittest.TestCase):

    def setUp(self):

        rm_dirs = ["/tmp/host0", "/tmp/host1"]
        mk_dirs = ["/tmp/host0", "/tmp/host1"]
        touch_files = [
            "/tmp/host1/testdata/input_paths/ecoclimap_bin_dir/ecoclimapI_covers_param.bin",
            "/tmp/host1/testdata/input_paths/ecoclimap_bin_dir/ecoclimapII_af_covers_param.bin",
            "/tmp/host1/testdata/input_paths/ecoclimap_bin_dir/ecoclimapII_eu_covers_param.bin",
            "/tmp/host1/testdata/input_paths/flake_dir/GlobalLakeDepth_V3.0.dir",
            "/tmp/host1/testdata/input_paths/flake_dir/GlobalLakeStatus_V3.0.dir",
            "/tmp/host1/testdata/input_paths/flake_dir/LAKE_LTA_NEW.nc",
            "/tmp/host1/testdata/input_paths/sand_dir/sand_fao.dir",
            "/tmp/host1/testdata/input_paths/sand_dir/sand_fao.hdr",
            "/tmp/host1/testdata/input_paths/clay_dir/clay_fao.hdr",
            "/tmp/host1/testdata/input_paths/clay_dir/clay_fao.hdr",
            "/tmp/host1/testdata/input_paths/soc_top_dir/soc_top.dir"
            "/tmp/host1/testdata/input_paths/soc_top_dir/soc_top.hdr",
            "/tmp/host1/testdata/input_paths/soc_sub_dir/soc_sub.dir",
            "/tmp/host1/testdata/input_paths/soc_sub_dir/soc_sub.hdr",
            "/tmp/host1/testdata/input_paths/ecoclimap_cover_dir/ECOCLIMAP_2_5_p.dir",
            "/tmp/host1/testdata/input_paths/oro_dir/gmted2010.dir",
            "/tmp/host1/testdata/input_paths/oro_dir/gmted2010.hdr",
            "/tmp/host1/scratch/sfx_home/EcflowContainers/climate/PGD.nc"
        ]
        copy_dirs = {"testdata": "/tmp/host1/testdata"}

        for d in rm_dirs:
            if os.path.exists(d):
                shutil.rmtree(d)

        for d in mk_dirs:
            os.makedirs(d, exist_ok=True)

        for target in copy_dirs:
            destination = copy_dirs[target] + "/"
            if os.path.exists(target):
                # os.makedirs(destination, exist_ok=True)
                shutil.copytree(target, destination)

        for f in touch_files:
            d = os.path.abspath(os.path.dirname(f))
            os.makedirs(d, exist_ok=True)
            os.system("touch " + f)

    def test_default(self):

        exp_name = "EcflowContainers"

        wd = "/tmp/host0/hm_wd/" + exp_name
        rev = experiment.__path__[0] + "/.."
        pysurfex_path = surfex.__path__[0] + "/.."

        dir_path = os.path.abspath(os.path.dirname(__file__))
        scheduler_path = dir_path
        sys.path.insert(0, scheduler_path)

        import scheduler

        os.makedirs(wd, exist_ok=True)
        argv = [
            "--wd", wd,
            "-surfex", pysurfex_path,
            "-rev", rev,
            "-host", "unittest",
            "--debug"
        ]
        kwargs = experiment_setup.parse_surfex_script_setup(argv)
        experiment_setup.surfex_script_setup(**kwargs)

        exp_dependencies_file = wd + "/paths_to_sync.json"
        sfx_exp = experiment.ExpFromFiles(exp_dependencies_file, stream=None)
        sfx_exp.write_scheduler_info(wd + "/scheduler.json")

        input_files = json.load(open(wd + "/exp_system.json", "r"))
        input_files["0"].update({"bin_dir": dir_path + "/bin"})
        input_files["1"].update({"bin_dir": dir_path + "/bin"})
        json.dump(input_files, open(wd + "/exp_system.json", "w"))

        orig_file = dir_path + "/../ecf/InitRun.py"
        test_file = wd + "/ecf/InitRun.py"
        fin = open(orig_file, "r")
        fout = open(test_file, "w")
        for line in fin:
            line = line.replace("%LIB%", wd)
            line = line.replace("%EXP_DIR%", wd)
            line = line.replace("%EXP_NAME%", exp_name)
            line = line.replace("%SUBMISSION_ID%", "")
            line = line.replace("%DTG%", "2022033003")
            line = line.replace("%DTGBEG%", "2022033000")
            line = line.replace("%ENSMBR%", "")
            line = line.replace("%ECF_TRYNO%", "1")
            line = line.replace("%ECF_RID%", "1")
            line = line.replace("%ECF_PASS%", "1")
            line = line.replace("%ECF_NAME%", "/Test/Test")
            line = line.replace("%TASK%", "InitRun")
            line = line.replace("%SERVER_LOGFILE%", "server.log")

            line = line.replace("%STREAM%", "")
            line = line.replace("@HOST_TO_BE_SUBSTITUTED@", "0")
            fout.write(line)
        fin.close()
        fout.close()

        loader = importlib.machinery.SourceFileLoader('__main__', test_file)
        mod = types.ModuleType(loader.name)
        loader.exec_module(mod)

        lib = sfx_exp.system.get_var("SFX_EXP_LIB", "1")
        orig_file = dir_path + "/../ecf/default.py"
        test_file = wd + "/ecf/default.py"
        fin = open(orig_file, "r")
        fout = open(test_file, "w")
        for line in fin:
            line = line.replace("%LIB%", lib)
            line = line.replace("%EXP_DIR%", wd)
            line = line.replace("%EXP_NAME%", exp_name)
            line = line.replace("%SUBMISSION_ID%", "")
            line = line.replace("%DTG%", "2022033003")
            line = line.replace("%DTGBEG%", "2022033000")
            line = line.replace("%ENSMBR%", "")
            line = line.replace("%ECF_TRYNO%", "1")
            line = line.replace("%ECF_RID%", "1")
            line = line.replace("%ECF_PASS%", "1")
            line = line.replace("%ECF_NAME%", "/Test/Test")
            line = line.replace("%TASK%", "Dummy")
            line = line.replace("%STREAM%", "")
            line = line.replace("%SERVER_LOGFILE%", "server.log")

            line = line.replace("@HOST_TO_BE_SUBSTITUTED@", "0")
            fout.write(line)
        fin.close()
        fout.close()

        loader = importlib.machinery.SourceFileLoader('__main__', test_file)
        mod = types.ModuleType(loader.name)
        loader.exec_module(mod)
        print(mod)

        # task_name = "LogProgress"
        task_name = "Prep"
        print("Running task " + task_name)
        classes = inspect.getmembers(experiment_tasks, inspect.isclass)
        task_class = None
        for c in classes:
            cname = c[0]
            ctask = c[1]
            if cname == task_name:
                task_class = ctask

        if task_class is None:
            raise Exception("Class not found for task " + task_name)

        host = "1"
        # The task knows which host it runs on and which member it is
        task_config = json.load(open(lib + "/exp_configuration.json", "r"))
        progress = {
            "DTG": "2022033003",
            "DTGBEG": "2022033000",
            "DTGPP": "2022033003"
        }

        ecf_name = "/TestSuite/TestFamily/SubFamily/" + task_name
        ecf_pass = "pass"
        ecf_tryno = "1"
        ecf_rid = "11"
        submission_id = "12"
        args = "%ARGS%"
        if args == "":
            args = None

        task = scheduler.EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid, submission_id)

        mbr = None
        stream = None
        args = None
        wrapper = ""
        system_variables = json.load(open(lib + "/exp_system_vars.json", "r"))[host]
        system_file_paths = json.load(open(lib + "/exp_system.json", "r"))[host]
        task_class(task, task_config, system_variables, system_file_paths, progress, mbr=mbr, stream=stream, args=args, debug=True).run(
            wrapper=wrapper)


class TestEcflowContainer2(unittest.TestCase):

    def test_default2(self):
        pass


'''
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
            "--wd", wd,
            "-surfex", surfex.__path__[0] + "/..",
            "-scheduler", scheduler.__path__[0],
            "-rev", rev,
            "-host", "unittest",
            "--debug" # ,
            # "--domain_file", "test/settings/conf_proj_test.json"
        ]
        kwargs = experiment.parse_surfex_script_setup(argv)
        experiment.surfex_script_setup(**kwargs)

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
'''
