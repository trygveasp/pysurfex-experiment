import os


class EcflowTask(object):

    def __init__(self, ecf_name, ecf_tryno, ecf_pass, ecf_rid, submission_id=None, ecf_timeout=20):
        print("EcflowTask")
        print(ecf_name, ecf_tryno, ecf_pass, ecf_rid, submission_id, ecf_timeout)
        self.ecf_name = ecf_name
        self.ecf_tryno = int(ecf_tryno)
        self.ecf_pass = ecf_pass
        if ecf_rid == "" or ecf_rid is None:
            ecf_rid = os.getpid()
        self.ecf_rid = int(ecf_rid)
        self.ecf_timeout = ecf_timeout
        ecf_name_parts = self.ecf_name.split("/")
        self.ecf_task = ecf_name_parts[-1]
        ecf_families = None
        if len(ecf_name_parts) > 2:
            ecf_families = ecf_name_parts[1:-1]
        self.ecf_families = ecf_families
        self.family1 = None
        if self.ecf_families is not None:
            self.family1 = self.ecf_families[-1]

        if submission_id == "":
            submission_id = None
        self.submission_id = submission_id


class EcflowClient(object):

    def __init__(self, server, task):
        print("Dummy EcflowClient")
        print(server, task)

    def __enter__(self):
        pass

    def __exit__(self, ex_type, value, tb):
        print("   Client:__exit__: ex_type:" + str(ex_type) + " value:" + str(value))
        if ex_type is not None:
            raise Exception("Client failed")


class SuiteDefinition(object):
    def __init__(self, suite_name, def_file, joboutdir, ecf_files, env_submit, server_config, server_log,
                 ecf_home=None, ecf_include=None, ecf_out=None, ecf_jobout=None,
                 ecf_job_cmd=None, ecf_status_cmd=None, ecf_kill_cmd=None, pythonpath="", path=""):
        print("Dummy SuiteDefinition")
        print(suite_name, def_file, joboutdir, ecf_files, env_submit, server_config, server_log)


class EcflowServerFromFile(object):
    def __init__(self, ecflow_server_file, logfile):
        print("Dummy EcflowServerFromFile")
        print(ecflow_server_file, logfile)
        self.settings = {}


class EcflowServer(object):
    def __init__(self, ecf_host, ecf_port, logfile):

        self.ecf_host = ecf_host
        self.ecf_port = ecf_port
        self.logfile = logfile
        self.ecf_client = EcflowClient(self.ecf_host, self.ecf_port)

    @staticmethod
    def start_server():
        print("Start EcFlow server")
