"""Dummy ecflow scheduler for testing."""
import os
import logging
import json
from abc import ABC, abstractmethod


# Base Scheduler server class
class Server(ABC):
    """Dummy base server."""

    def __init__(self):
        """Construct server."""
        self.settings = None

    @abstractmethod
    def start_server(self):
        """Start server abstract method.

        Raises:
            NotImplementedError: _description_
        """
        raise NotImplementedError

    @abstractmethod
    def replace(self, suite_name, def_file):
        """Replace.

        Args:
            suite_name (_type_): _description_
            def_file (_type_): _description_

        Raises:
            NotImplementedError: _description_
        """
        raise NotImplementedError

    @abstractmethod
    def begin_suite(self, suite_name):
        """Begin suite.

        Args:
            suite_name (_type_): _description_

        Raises:
            NotImplementedError: _description_
        """
        raise NotImplementedError

    def start_suite(self, suite_name, def_file, begin=True):
        """Start suite.

        Args:
            suite_name (_type_): _description_
            def_file (_type_): _description_
            begin (bool, optional): _description_. Defaults to True.
        """
        self.start_server()
        self.replace(suite_name, def_file)
        if begin:
            self.begin_suite(suite_name)

    def force_aborted(self, task):
        """Force task aborted."""


class EcflowTask(object):
    """Dummy ecflow task."""

    def __init__(self, ecf_name, ecf_tryno, ecf_pass, ecf_rid, submission_id=None, ecf_timeout=20):
        """Construct ecflow task.

        Args:
            ecf_name (str): _description_
            ecf_tryno (int): _description_
            ecf_pass (str): _description_
            ecf_rid (int): _description_
            submission_id (str, optional): _description_. Defaults to None.
            ecf_timeout (int, optional): _description_. Defaults to 20.
        """
        logging.debug("EcflowTask: %s", ecf_name)
        logging.debug("%s %s %s %s %s", str(ecf_tryno), ecf_pass, str(ecf_rid),
                      str(submission_id), str(ecf_timeout))
        self.ecf_name = ecf_name
        # self.ecf_tryno = int(ecf_tryno)
        self.ecf_tryno = ecf_tryno
        self.ecf_pass = ecf_pass
        if ecf_rid == "" or ecf_rid is None:
            ecf_rid = os.getpid()
        # self.ecf_rid = int(ecf_rid)
        self.ecf_rid = ecf_rid
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
    """Dummy ecflow client."""

    def __init__(self, server, task):
        """Constuct a client.

        Args:
            server (_type_): _description_
            task (_type_): _description_
        """
        logging.debug("Dummy EcflowClient. server=%s task=%s", server, task)
        self.server = server
        self.task = task

    def __enter__(self):
        """Enter method."""
        logging.debug("Start signal for %s", str(self.task))

    def __exit__(self, ex_type, value, traceback):
        """Exit the method on success or failure."""
        logging.debug("   Client:__exit__: ex_type: %s value: %s", str(ex_type), str(value))
        if ex_type is not None:
            logging.debug("Traceback: %s", str(traceback))
            raise Exception("Client failed")


class EcfNode():
    """Emulate Ecflow internal node."""

    def __init__(self):
        """Initialize EcfNode."""

    def add_variable(self, key, value):
        """Add a variable.

        Args:
            key (_type_): _description_
            value (_type_): _description_
        """
        logging.debug("Added %s=%s", key, value)


class EcflowNode():
    """Dummy ecflow node."""

    def __init__(self, name, node_type, parent, **kwargs):
        """Initialize ecflow node.

        Args:
            name (_type_): _description_
            node_type (_type_): _description_
            parent (_type_): _description_
        """
        logging.debug("Construct dummy ecflow node: %s %s %s Args: %s", name, node_type,
                      str(parent), str(kwargs))
        self.ecf_node = EcfNode()

    def add_variable(self, key, value):
        """Add a node variable.

        Args:
            key (_type_): _description_
            value (_type_): _description_
        """
        logging.debug("Added %s=%s", key, value)


class EcflowSuite():
    """Dummy Ecflowsuite."""

    def __init__(self, name, variables=None, **kwargs):
        """Initialize suite.

        Args:
            name (_type_): _description_
            variables (_type_, optional): _description_. Defaults to None.
        """
        self.name = name
        self.variable = variables
        self.ecf_node = EcflowNode(name, "suite", None, **kwargs)

    def save_as_defs(self, def_file):
        """Save as defs.

        Args:
            def_file (_type_): _description_
        """
        logging.debug("Save definitions for %s to %s", self.name, def_file)


class SuiteDefinition():
    """Dummy suite definition."""

    def __init__(self, suite_name, joboutdir, ecf_files, env_submit,
                 ecf_home=None, ecf_include=None, ecf_out=None, ecf_jobout=None,
                 ecf_job_cmd=None, ecf_status_cmd=None, ecf_kill_cmd=None, pythonpath="", path=""):
        """Construct an experiment suite.

        Args:
            suite_name (_type_): _description_
            joboutdir (_type_): _description_
            ecf_files (_type_): _description_
            env_submit (_type_): _description_
            ecf_home (_type_, optional): _description_. Defaults to None.
            ecf_include (_type_, optional): _description_. Defaults to None.
            ecf_out (_type_, optional): _description_. Defaults to None.
            ecf_jobout (_type_, optional): _description_. Defaults to None.
            ecf_job_cmd (_type_, optional): _description_. Defaults to None.
            ecf_status_cmd (_type_, optional): _description_. Defaults to None.
            ecf_kill_cmd (_type_, optional): _description_. Defaults to None.
            pythonpath (str, optional): _description_. Defaults to "".
            path (str, optional): _description_. Defaults to "".
        """
        logging.debug("Dummy SuiteDefinition")
        logging.debug("suite_name=%s", suite_name)
        logging.debug("joboutdir=%s", joboutdir)
        logging.debug("ecf_files=%s", ecf_files)
        logging.debug("env_submit=%s", env_submit)
        logging.debug("ecf_home=%s", ecf_home)
        logging.debug("ecf_include=%s", ecf_include)
        logging.debug("ecf_out=%s", ecf_out)
        logging.debug("ecf_jobout=%s", ecf_jobout)
        logging.debug("ecf_job_cmd=%s", ecf_job_cmd)
        logging.debug("ecf_status_cmd=%s", ecf_status_cmd)
        logging.debug("ecf_kill_cmd=%s", ecf_kill_cmd)
        logging.debug("path=%s", path)
        logging.debug("pythonpath=%s", pythonpath)
        variables = None
        self.suite = EcflowSuite(suite_name, variables=variables)

    def save_as_defs(self, def_file):
        """Save definition file.

        Args:pythonpath="", path=""
            def_file (_type_): _description_
        """
        self.suite.save_as_defs(def_file)


class EcflowServer(Server):
    """Dummy ecflow server."""

    def __init__(self, ecf_host, ecf_port, logfile):
        """Construct ecflow server.

        Args:
            ecf_host (_type_): _description_
            ecf_port (_type_): _description_
            logfile (_type_): _description_
        """
        Server.__init__(self)
        self.ecf_host = ecf_host
        self.ecf_port = ecf_port
        self.logfile = logfile
        self.ecf_client = EcflowClient(self.ecf_host, self.ecf_port)
        self.settings = {
            "ECF_HOST": self.ecf_host,
            "ECF_PORT": self.ecf_port
        }

    def start_server(self):
        """Start server."""
        logging.debug("Start EcFlow server")

    def replace(self, suite_name, def_file):
        """Replace suite.

        Args:
            suite_name (_type_): _description_
            def_file (_type_): _description_
        """
        logging.debug("Replace suite: %s %s", suite_name, def_file)

    def begin_suite(self, suite_name):
        """Begin suite.

        Args:
            suite_name (_type_): _description_
        """
        logging.debug("Begin suite: %s", suite_name)


class EcflowServerFromFile(EcflowServer):
    """Dummy ecflow server from a ascii file."""

    def __init__(self, ecflow_server_file, logfile):
        """Constuct the EcflowServer.

        Args:
            ecflow_server_file (_type_): _description_
            logfile (_type_): _description_

        Raises:
            FileNotFoundError: _description_
        """
        logging.debug("Dummy EcflowServerFromFile %s,%s", ecflow_server_file, logfile)
        if os.path.exists(ecflow_server_file):
            with open(ecflow_server_file, mode="r", encoding="UTF-8") as file_handler:
                self.settings = json.load(file_handler)
        else:
            raise FileNotFoundError("Could not find " + ecflow_server_file)

        ecf_host = self.settings.get("ECF_HOST")
        ecf_port_offset = self.settings.get("ECF_PORT_OFFSET")
        if ecf_port_offset is None:
            ecf_port_offset = 1500
        else:
            ecf_port_offset = int(ecf_port_offset)
        ecf_port = self.settings.get("ECF_PORT")
        if ecf_port is None:
            ecf_port = int(os.getuid())
        else:
            ecf_port = int(ecf_port)
        ecf_port = ecf_port + ecf_port_offset

        EcflowServer.__init__(self, ecf_host, ecf_port, logfile)


class EcflowNodeContainer(EcflowNode):
    """Node container."""

    def __init__(self, name, node_type, parent, **kwargs):
        """Construct node container.

        Args:
            name (_type_): _description_
            node_type (_type_): _description_
            parent (_type_): _description_
        """
        EcflowNode.__init__(self, name, node_type, parent, **kwargs)


class EcflowSuiteTriggers(object):
    """Dummy ecflow triggers."""

    def __init__(self, triggers, **kwargs):
        """Construct triggers.

        Args:
            triggers (_type_): _description_
        """
        logging.debug("Constuct EcflowSuiteTriggers %s kwargs=%s", str(triggers), str(kwargs))

    def add_triggers(self, triggers, mode="AND"):
        """Add triggers.

        Args:
            triggers (_type_): _description_
            mode (str, optional): _description_. Defaults to "AND".
        """
        logging.debug("Add triggers %s %s", str(triggers), mode)


class EcflowSuiteTrigger(object):
    """EcFlow Trigger in a suite."""

    def __init__(self, node, mode="complete"):
        """Create a EcFlow trigger object.

        Args:
            node (scheduler.EcflowNode): The node to trigger on
            mode (str):
        """
        self.node = node
        self.mode = mode


class EcflowSuiteVariable(object):
    """Dummy ecflow suie variable."""

    def __init__(self, name, value):
        """Construct dummy ecflow suite variable.

        Args:
            name (_type_): _description_
            value (_type_): _description_
        """
        self.name = name
        self.value = value


class EcflowSuiteFamily(EcflowNodeContainer):
    """Dummy ecflow suite family.

    Args:
        EcflowNodeContainer (_type_): _description_
    """

    def __init__(self, name, parent, **kwargs):
        """Construct a family in ecflow.

        Args:
            name (_type_): _description_
            parent (_type_): _description_
        """
        EcflowNodeContainer.__init__(self, name, "family", parent, **kwargs)


class EcflowSuiteTask(EcflowNode):
    """Dummy ecflow task.

    Args:
        EcflowNode (_type_): _description_
    """

    def __init__(self, name, parent, **kwargs):
        """Construct ecflow task.

        Args:
            name (_type_): _description_
            parent (_type_): _description_
        """
        EcflowNode.__init__(self, name, "task", parent, **kwargs)


class EcflowSubmitTask(object):
    """Submit class for ecflow."""

    def __init__(self, task, env_submit, server, joboutdir,
                 stream=None, dbfile=None, interpreter="#!/usr/bin/env python3",
                 ensmbr=None, submit_exceptions=None, coldstart=False, env_file=None,
                 communication=True):
        """Submit an ecflow task.

        Args:
            task (_type_): _description_
            env_submit (_type_): _description_
            server (_type_): _description_
            joboutdir (_type_): _description_
            stream (_type_, optional): _description_. Defaults to None.
            dbfile (_type_, optional): _description_. Defaults to None.
            interpreter (str, optional): _description_. Defaults to "#!/usr/bin/env python3".
            ensmbr (_type_, optional): _description_. Defaults to None.
            submit_exceptions (_type_, optional): _description_. Defaults to None.
            coldstart (bool, optional): _description_. Defaults to False.
            env_file (_type_, optional): _description_. Defaults to None.
            communication (bool, optional): _description_. Defaults to True.
        """

    def submit(self):
        """Submit method."""


class TaskSettings(object):
    """Dummy task specific setttings."""

    def __init__(self, task, submission_defs, joboutdirs, submit_exceptions=None,
                 interpreter="#!/usr/bin/env python3", complete=False, coldstart=False):
        """Construct an object with task settings.

        Args:
            task (_type_): _description_
            submission_defs (_type_): _description_
            joboutdirs (_type_): _description_
            submit_exceptions (_type_, optional): _description_. Defaults to None.
            interpreter (str, optional): _description_. Defaults to "#!/usr/bin/env python3".
            complete (bool, optional): _description_. Defaults to False.
            coldstart (bool, optional): _description_. Defaults to False.

        Raises:
            e: _description_

        Returns:
            _type_: _description_
        """


class SubmissionBaseClass():
    """Dummy submission class."""

    def __init__(self, task, task_settings, server, db=None, remote_submit_cmd=None,
                 remote_kill_cmd=None, remote_status_cmd=None):
        """Constuct a submisssion object.

        Args:
            task (_type_): _description_
            task_settings (_type_): _description_
            server (_type_): _description_
            db (_type_, optional): _description_. Defaults to None.
            remote_submit_cmd (_type_, optional): _description_. Defaults to None.
            remote_kill_cmd (_type_, optional): _description_. Defaults to None.
            remote_status_cmd (_type_, optional): _description_. Defaults to None.
        """

    def status(self):
        """Query status of task."""

    def kill(self):
        """Kill the task."""


def get_submission_object(task, task_settings, server=None, db_file=None):
    """Get the submssion object.

    Args:
        task (_type_): _description_
        task_settings (_type_): _description_
        server (_type_, optional): _description_. Defaults to None.
        db_file (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    return SubmissionBaseClass(task, task_settings, server, db=db_file)


def submit_cmd(**kwargs):
    """Submit dummy method.

    Raises:
        e: _description_
    """
    try:
        ecf_name = kwargs["ecf_name"]
        ensmbr = kwargs["ensmbr"]
        ecf_tryno = kwargs["ecf_tryno"]
        ecf_pass = kwargs["ecf_pass"]
        ecf_rid = kwargs["ecf_rid"]
    except KeyError as ex:
        raise ex
        # raise SubmitException("You are missing needed keys")

    joboutdir = kwargs["joboutdir"]
    if isinstance(joboutdir, str):
        joboutdir = {"0": joboutdir}
    env_submit = kwargs["env_submit"]
    if isinstance(env_submit, str):
        env_submit = json.load(open(env_submit, mode="r", encoding="UTF-8"))
    server = kwargs["env_server"]
    logfile = kwargs["logfile"]
    if isinstance(server, str):
        server = EcflowServerFromFile(server, logfile=logfile)
    if isinstance(server, dict):
        server = EcflowServer(server["ECF_HOST"], server["ECF_PORT"], logfile=logfile)
    env_file = kwargs.get("env_file")

    submission_id = None
    stream = kwargs.get("stream")
    dbfile = kwargs.get("dbfile")
    coldstart = kwargs.get("coldstart")
    if coldstart is None:
        coldstart = False

    if ecf_rid is not None:
        if ecf_rid == "":
            ecf_rid = os.getpid()
    else:
        ecf_rid = os.getpid()

    dry_run = kwargs.get("dry_run")
    if dry_run is None:
        dry_run = False

    try:
        task = EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid, submission_id)
        sub = EcflowSubmitTask(task, env_submit, server, joboutdir, env_file=env_file,
                               ensmbr=ensmbr,
                               dbfile=dbfile, stream=stream, coldstart=coldstart)
        if not dry_run:
            sub.submit()
    except Exception as ex:
        raise ex


def kill_cmd(**kwargs):
    """Kill the task."""
    ecf_name = kwargs["ecf_name"]
    ecf_tryno = kwargs["ecf_tryno"]
    ecf_pass = kwargs["ecf_pass"]
    ecf_rid = kwargs["ecf_rid"]
    submission_id = kwargs.get("submission_id")
    if submission_id == "":
        submission_id = None
    env_submit = kwargs["env_submit"]
    if isinstance(env_submit, str):
        env_submit = json.load(open(env_submit, mod="r", encoding="UTF-8"))
    jobout_dir = kwargs["joboutdir"]
    if isinstance(jobout_dir, str):
        jobout_dir = {"0": jobout_dir}

    server = kwargs["env_server"]
    # If a server environment file, create a server
    if isinstance(server, str):
        logfile = kwargs["logfile"]
        server = EcflowServerFromFile(server, logfile)
    if isinstance(server, dict):
        logfile = kwargs["logfile"]
        server = EcflowServer(server["ECF_HOST"], server["ECF_PORT"], logfile)

    dry_run = False
    if "dry_run" in kwargs:
        dry_run = kwargs["dry_run"]

    task = EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid, submission_id)
    task_settings = TaskSettings(task, env_submit, jobout_dir)
    sub = get_submission_object(task, task_settings, server=server)
    if not dry_run:
        sub.kill()
        server.force_aborted(task)


def status_cmd(**kwargs):
    """Set status."""
    ecf_name = kwargs["ecf_name"]
    ecf_tryno = kwargs["ecf_tryno"]
    ecf_pass = kwargs["ecf_pass"]
    ecf_rid = kwargs["ecf_rid"]
    submission_id = kwargs["submission_id"]
    env_submit = kwargs["env_submit"]
    if isinstance(env_submit, str):
        env_submit = json.load(open(env_submit, mode="r", encoding="UTF-8"))
    jobout_dir = kwargs["joboutdir"]
    if isinstance(jobout_dir, str):
        jobout_dir = {"0": jobout_dir}

    dry_run = False
    if "dry_run" in kwargs:
        dry_run = kwargs["dry_run"]

    task = EcflowTask(ecf_name, ecf_tryno, ecf_pass, ecf_rid, submission_id)
    task_settings = TaskSettings(task, env_submit, jobout_dir)

    sub = get_submission_object(task, task_settings)
    if not dry_run:
        sub.status()
