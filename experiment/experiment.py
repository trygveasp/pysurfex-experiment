"""Experiment classes and methods."""
import os
import json
import logging
from datetime import timedelta
import collections
import copy
import scheduler
import toml
import experiment


class ExpConfiguration():
    """The main experiment configuration.

    It is based on a merged general configuration and a member specific configuration.
    Contrary to the surfex configuration which is a dict with the actual configuration for the
    given member etc.

    """

    def __init__(self, config):
        """Constuct object used to write experiment config files and create suite definition.

        Args:
            config (dict):

        """
        conf_dict, member_conf_dict = self.split_member_settings(config)
        self.settings = conf_dict
        self.config_file = None

        # Find EPS information
        self.members = None
        if "FORECAST" in self.settings:
            if "ENSMSEL" in self.settings["FORECAST"]:
                self.members = self.get_setting("FORECAST#ENSMSEL")
                if len(self.members) == 0:
                    self.members = None
        self.member_settings = None
        self.task_limit = None

        member_settings = {}
        # Set EPS config
        if self.members is not None:
            for mbr in self.members:

                if str(mbr) in member_conf_dict:
                    mbr_configs = member_conf_dict[str(mbr)]
                else:
                    raise Exception("Could not find config for member " + str(mbr))
                member_settings.update({str(mbr): mbr_configs})
        self.member_settings = member_settings

        # Update time information
        cycle_times = self.get_cycle_list()
        fcint = {}
        fgint = {}
        for cycle in cycle_times:
            cycle_delta = timedelta(hours=int(cycle))
            cycle_string = self.format_cycle_string(cycle_delta)
            fcint.update({cycle_string: int(self.set_fcint(cycle_delta).total_seconds())})
            fgint.update({cycle_string: int(self.set_fgint(cycle_delta).total_seconds())})
        logging.debug("fcint %s", str(fcint))
        logging.debug("fgint %s", str(fgint))
        self.settings["GENERAL"].update({"FCINT": fcint})
        self.settings["GENERAL"].update({"FGINT": fgint})
        if self.members is not None:
            for mbr in self.members:
                fcint_members = {}
                fgint_members = {}
                cycle_times = self.get_cycle_list(mbr=mbr)
                for cycle in cycle_times:
                    cycle_delta = timedelta(hours=int(cycle))
                    cycle_string = self.format_cycle_string(cycle_delta)
                    secs = int(self.set_fcint(cycle_delta, mbr=mbr).total_seconds())
                    fcint_members.update({cycle_string: secs})
                    secs = int(self.set_fgint(cycle_delta, mbr=mbr).total_seconds())
                    fgint_members.update({cycle_string: secs})
                self.member_settings[str(mbr)]["GENERAL"].update({"FCINT": fcint_members})
                self.member_settings[str(mbr)]["GENERAL"].update({"FGINT": fgint_members})
                logging.debug("fcint_members %s", str(fcint_members))
                logging.debug("fgint_members %s", str(fgint_members))

        # self.do_build = self.setting_is("COMPILE#BUILD", "yes")
        self.ecoclimap_sg = self.setting_is("SURFEX#COVER#SG", True)
        self.gmted = self.setting_is("SURFEX#ZS#YZS", "gmted2010.dir")

        self.ekf = self.setting_is("SURFEX#ASSIM#SCHEMES#ISBA", "EKF")
        nncv = self.get_setting("SURFEX#ASSIM#ISBA#EKF#NNCV")
        perts = []
        for pert_number, value in enumerate(nncv):
            if value == 1:
                perts.append(pert_number)
        self.perts = perts

        # Stream
        stream = None
        if "STREAM" in self.settings:
            stream = self.get_setting("GENERAL#STREAM")

        # System variables
        system = self.settings["SYSTEM_VARS"]
        print(system)
        exp_name = self.get_setting("GENERAL#EXP")
        exp_dir = self.get_setting("GENERAL#EXP_DIR")
        self.system = experiment.System(system, exp_name)

        # System file paths
        system_file_paths = self.settings["SYSTEM_FILE_PATHS"]
        system_file_paths = experiment.SystemFilePathsFromSystem(system_file_paths, self.system,
                                                                      hosts=self.system.hosts,
                                                                      stream=stream, wdir=exp_dir)
        # self.settings.update({"EXP_SYSTEM_FILE_PATHS": self.system_file_paths.paths["0"]})
        # TODO handle host
        self.exp_file_paths = system_file_paths.paths["0"]

        server = self.settings["SCHEDULER"]
        self.server = scheduler.EcflowServer(ecf_host=server["ECF_HOST"], ecf_port=server["ECF_PORT"])
        self.env_submit = self.settings["SUBMISSION"]
        # Date/time
        progress = self.settings["PROGRESS"]
        self.progress =  experiment.ProgressFromDict(progress)


    @staticmethod
    def split_member_settings(merged_settings):
        """Process the settings and split out member settings.

        Args:
            merged_settings (dict): dict with all settings

        Returns:
            (dict, dict): General config, member_config

        """
        # Write member settings
        members = None
        if "FORECAST" in merged_settings:
            if "ENSMSEL" in merged_settings["FORECAST"]:
                members = list(merged_settings["FORECAST"]["ENSMSEL"])

        logging.debug("Merged settings %s", merged_settings)
        merged_member_settings = {}
        if "EPS" in merged_settings:
            if "MEMBER_SETTINGS" in merged_settings["EPS"]:
                merged_member_settings = copy.deepcopy(merged_settings["EPS"]["MEMBER_SETTINGS"])
                logging.debug("Found member settings %s", merged_member_settings)
                del merged_settings["EPS"]["MEMBER_SETTINGS"]

        member_settings = {}
        if members is not None:
            for mbr in members:
                toml_settings = copy.deepcopy(merged_settings)
                logging.debug("member: %s merged_member_dict: %s", str(mbr), merged_member_settings)
                member_dict = ExpConfiguration.get_member_settings(merged_member_settings, mbr)
                logging.debug("member_dict: %s", member_dict)
                toml_settings = ExpConfiguration.merge_toml_env(toml_settings, member_dict)
                member_settings.update({str(mbr): toml_settings})

        return merged_settings, member_settings


    @staticmethod
    def deep_update(source, overrides):
        """Update a nested dictionary or similar mapping.

        Modify ``source`` in place.

        Args:
            source (_type_): _description_
            overrides (_type_): _description_

        Returns:
            _type_: _description_

        """
        for key, value in overrides.items():
            if isinstance(value, collections.Mapping) and value:
                returned = ExpConfiguration.deep_update(source.get(key, {}), value)
                source[key] = returned
            else:
                override = overrides[key]

                source[key] = override

        return source


    @staticmethod
    def merge_toml_env(old_env, mods):
        """Merge the dicts from toml by a deep update.

        Args:
            old_env (_type_): _description_
            mods (_type_): _description_

        Returns:
            _type_: _description_

        """
        return ExpConfiguration.deep_update(old_env, mods)


    @staticmethod
    def flatten(d, sep="#"):
        """Flatten the setting to get member setting.

        Args:
            d (_type_): _description_
            sep (str, optional): Separator. Defaults to "#".

        Returns:
            _type_: _description_

        """
        obj = collections.OrderedDict()

        def recurse(t, parent_key=""):
            if isinstance(t, list):
                for i in enumerate(t):
                    recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
            elif isinstance(t, dict):
                for k, v in t.items():
                    recurse(v, parent_key + sep + k if parent_key else k)
            else:
                obj[parent_key] = t

        recurse(d)
        return obj


    @staticmethod
    def get_member_settings(d, member, sep="#"):
        """Get the member setting.

        Args:
            d (_type_): _description_
            member (_type_): _description_
            sep (str, optional): _description_. Defaults to "#".

        Returns:
            _type_: _description_

        """
        member_settings = {}
        settings = ExpConfiguration.flatten(d)
        for setting in settings:
            keys = setting.split(sep)
            logging.debug("Keys: %s", str(keys))
            if len(keys) == 1:
                member3 = f"{int(member):03d}"
                val = settings[setting]
                if isinstance(val, str):
                    val = val.replace("@EEE@", member3)

                this_setting = {keys[0]: val}
                member_settings = ExpConfiguration.merge_toml_env(member_settings, this_setting)
            else:
                this_member = int(keys[-1])
                keys = keys[:-1]
                logging.debug("This member: %s member=%s Keys=%s", str(this_member), str(member), keys)
                if int(this_member) == int(member):
                    this_setting = settings[setting]
                    for key in reversed(keys):
                        this_setting = {key: this_setting}

                    logging.debug("This setting: %s", str(this_setting))
                    member_settings = ExpConfiguration.merge_toml_env(member_settings, this_setting)
                    logging.debug("Merged member settings for member %s = %s",
                                str(member), str(member_settings))
        logging.debug("Finished member settings for member %s = %s", str(member), str(member_settings))
        return member_settings

    @staticmethod
    def format_cycle_string(cycle_delta):
        """Format the cycle string.

        Args:
            cycle_delta (datetime.timedelta): The time delta from midnight.

        Returns:
            str: Formatted string
        """
        secs = cycle_delta.total_seconds()
        hours = int(secs / 3600)
        mins = int(secs % 3600)
        return f"{hours:02}{mins:02}"

    def dump_json(self, filename, indent=None):
        """Dump a json file with configuration.

        Args:
            filename (str): Filename of json file to write
            indent (int): Indentation in filename

        Returns:
            None

        """
        if json is None:
            raise Exception("json module not loaded")

        if self.members is not None:
            settings = self.member_settings.copy()
        else:
            settings = self.settings.copy()

        logging.debug("Settings: %s", str(settings))
        with open(filename, mode="w", encoding="UTF-8") as file_handler:
            json.dump(settings, file_handler, indent=indent)

    def max_fc_length(self, mbr=None):
        """Calculate the max forecast time.

        Args:
            mbr (int, optional): ensemble member. Defaults to None.

        Returns:
            _type_: _description_
        """
        ll_list = self.get_lead_time_list(mbr=mbr)
        max_fc = -1
        for ll_val in ll_list:
            if int(ll_val) > int(max_fc):
                max_fc = int(ll_val)
        return max_fc

    def setting_is(self, setting, value, **kwargs):
        """Check if setting is value.

        Args:
            setting (_type_): _description_
            value (_type_): _description_

        Returns:
            bool: True if found, False if not found.
        """
        if self.get_setting(setting, **kwargs) == value:
            return True
        return False

    def setting_is_not(self, setting, value, **kwargs):
        """Check if setting is not value.

        Args:
            setting (_type_): _description_
            value (_type_): _description_

        Returns:
            bool: True if not found, False if found.

        """
        found = False
        if self.get_setting(setting, **kwargs) == value:
            found = True

        if found:
            return False
        return True

    def value_is_one_of(self, setting, value, **kwargs):
        """Check if the setting contains value.

        Args:
            setting (_type_): _description_
            value (list): _description_

        Returns:
            bool: True if found, False if not found.

        """
        found = False
        setting = self.get_setting(setting, **kwargs)
        for test_setting in setting:
            if test_setting == value:
                found = True
        return found

    def value_is_not_one_of(self, setting, value, **kwargs):
        """Check if the setting does not contain value.

        Args:
            setting (_type_): _description_
            value (list): _description_

        Returns:
            bool: True if not found, False if found.

        """
        found = self.value_is_one_of(setting, value, **kwargs)
        if found:
            return False
        return True

    def setting_is_one_of(self, setting, values, **kwargs):
        """Check if setting is one of the provided list of values.

        Args:
            setting (_type_): _description_
            values (_type_): _description_

        Raises:
            Exception: _description_

        Returns:
            bool: True if found, False if not found.

        """
        found = False
        setting = self.get_setting(setting, **kwargs)
        if not isinstance(values, list):
            raise Exception("Excpected a list as input, got ", type(values))
        for val in values:
            if setting == val:
                found = True
        return found

    def setting_is_not_one_of(self, setting, values, **kwargs):
        """Check if setting is not one of the provided list of values.

        Args:
            setting (_type_): _description_
            values (_type_): _description_

        Returns:
            bool: True if not found, False if found.

        """
        found = self.setting_is_one_of(setting, values, **kwargs)
        if found:
            return False
        return True

    def get_setting(self, setting, **kwargs):
        """Get the setting (and possibly extrapolate it).

        Args:
            setting (_type_): _description_

        Raises:
            Exception: _description_
            Exception: _description_
            KeyError: _description_
            KeyError: _description_

        Returns:
            _type_: value
        """
        mbr = None
        if "mbr" in kwargs:
            if kwargs["mbr"] is not None:
                mbr = str(kwargs["mbr"])
        sep = kwargs.get("sep")
        if sep is None:
            sep = "#"

        abort = kwargs.get("abort")
        if abort is None:
            abort = True
        default = kwargs.get("default")

        if mbr is None:
            settings = self.settings
        else:
            if self.members is not None:
                if str(mbr) in self.members:
                    settings = self.member_settings[str(mbr)]
                else:
                    raise Exception("Not a valid member: " + str(mbr))
            else:
                raise Exception("No members found")

        if sep is None:
            keys = [setting]
        else:
            keys = setting.split(sep)

        if keys[0] in settings:
            this_setting = settings[keys[0]]
            logging.debug("get_setting %s -> %s", keys[0], str(this_setting))
            if len(keys) > 1:
                for key in keys[1:]:
                    if key in this_setting:
                        this_setting = this_setting[key]
                    else:
                        if default is not None:
                            this_setting = default
                        elif abort:
                            raise KeyError("Key not found " + key)
                        else:
                            this_setting = None
        else:
            if abort:
                raise KeyError("Key not found " + keys[0])
            this_setting = None

        logging.debug("get_setting %s %s %s %s", str(setting), str(this_setting), str(mbr),
                      type(this_setting))
        return this_setting

    def update_setting(self, setting, value, mbr=None, sep="#"):
        """Update the setting.

        Args:
            setting (_type_): _description_
            value (_type_): _description_
            mbr (_type_, optional): _description_. Defaults to None.
            sep (str, optional): _description_. Defaults to "#".

        Raises:
            Exception: _description_

        """
        if sep is None:
            keys = [setting]
        else:
            keys = setting.split(sep)

        last_key = keys[-1]
        dsetting = {last_key: value}
        if len(keys) > 1:
            for key in reversed(keys[0:-1]):
                dsetting = {key: dsetting}

        if mbr is None:
            self.settings = self.merge_toml_env(self.settings, dsetting)
        else:
            if self.members is not None and str(mbr) in self.members:
                self.member_settings[str(mbr)] = \
                    self.merge_toml_env(self.member_settings[str(mbr)], dsetting)
            else:
                raise Exception("Not a valid member: " + str(mbr))

    def get_total_unique_cycle_list(self):
        """Get a list of unique start times for the forecasts.

        Returns:
            list: List with times
        """
        # Create a list of all unique HHs from all members
        # print(self.members, self.get_hh_list())
        hh_list_all = []
        if self.members is not None:
            for mbr in self.members:
                hh_l = self.get_cycle_list(mbr=mbr)
                for hour in hh_l:
                    hour = f"{int(hour):02d}"
                    if hour not in hh_list_all:
                        hh_list_all.append(hour)
        else:
            hh_l = self.get_cycle_list()
            for hour in hh_l:
                hour = f"{int(hour):02d}"
                if hour not in hh_list_all:
                    hh_list_all.append(hour)

        # print(hh_list_all)
        # Sort this list
        hh_list = []
        for hour in sorted(hh_list_all):
            hh_list.append(hour)

        return hh_list

    def get_fgint(self, dtg, mbr=None):
        """Get the fgint.

        Args:
            dtg (datetime.datetime): Cycle time.
            mbr (int, optional): Ensemble member. Defaults to None.

        Returns:
            int: fgint in seconds

        """
        time_stamp = dtg.strftime("%H%M")
        fgints = self.get_setting("GENERAL#FGINT", mbr=mbr)
        fgint = fgints.get(time_stamp)
        logging.debug("FGINT=%s DTG=%s fgints=%s time_stamp=%s", fgint, dtg, str(fgints),
                      time_stamp)
        return fgint

    def get_fcint(self, dtg, mbr=None):
        """Get the fcint.

        Args:
            dtg (datetime.datetime): Cycle time.
            mbr (int, optional): Ensemble member. Defaults to None.

        Returns:
            int: fcint in seconds

        """
        time_stamp = dtg.strftime("%H%M")
        fcints = self.get_setting("GENERAL#FCINT", mbr=mbr)
        fcint = fcints.get(time_stamp)
        logging.debug("FCINT=%s DTG=%s fcints=%s time_stamp=%s", fcint, dtg, str(fcints),
                      time_stamp)
        return fcint

    def set_fgint(self, cycle, mbr=None):
        """Set the the interval between the forecasts before.

        Args:
            cycle (datetime.timedelta): Timedelta from midnight
            mbr (int, optional): ensemble member. Defaults to None.

        Returns:
            datetime.timedelta: First guess intervall

        """
        hh_list = self.get_cycle_list(mbr=mbr)
        prev_delta = None
        for delta, val in enumerate(hh_list):
            val = timedelta(hours=int(val))
            logging.debug("delta=%s val=%s cycle=%s", delta, val, cycle)
            if val == cycle:
                if prev_delta is not None:
                    return val - prev_delta
            prev_delta = val
        # 24 hours timdelta if we only have one cycle pr day.
        if len(hh_list) == 1:
            prev_delta = timedelta(hours=0)
        return timedelta(hours=24) - prev_delta

    def set_fcint(self, cycle, mbr=None):
        """Set the the interval between the forecasts after.

        Args:
            cycle (datetime.timedelta): Timedelta from midnight
            mbr (int, optional): ensemble member. Defaults to None.

        Returns:
            datetime.timedelta: Interval to  next forecast.

        """
        hh_list = self.get_cycle_list(mbr=mbr)
        start_delta = None
        first_val = None
        for delta, val in enumerate(hh_list):
            val = timedelta(hours=int(val))
            logging.debug("delta=%s val=%s cycle=%s", delta, val, cycle)
            if start_delta is not None:
                return val - start_delta
            if val == cycle:
                start_delta = val
            if first_val is None:
                first_val = val

        # 24 hours timdelta if we only have one cycle pr day.
        if len(hh_list) == 1:
            start_delta = timedelta(hours=0)
        return timedelta(hours=24) - start_delta

    def get_cycle_list(self, mbr=None):
        """Get a list of forecast start times.

        Args:
            mbr (int, optional): ensemble member. Defaults to None.

        Returns:
            list: Forecast start times.

        """
        hh_list = self.get_setting("GENERAL#HH_LIST", mbr=mbr)
        ll_list = self.get_setting("GENERAL#LL_LIST", mbr=mbr)
        hh_list, ll_list = self.expand_hh_and_ll_list(hh_list, ll_list)
        logging.debug("hh_list: %s", hh_list)
        return hh_list

    def get_lead_time_list(self, mbr=None):
        """Get a list of forecast lead times.

        Args:
            mbr (int, optional): ensemble member. Defaults to None.

        Returns:
            list: Forecast lead times.

        """
        hh_list = self.get_setting("GENERAL#HH_LIST", mbr=mbr)
        ll_list = self.get_setting("GENERAL#LL_LIST", mbr=mbr)
        hh_list, ll_list = self.expand_hh_and_ll_list(hh_list, ll_list)
        logging.debug("ll_list: %s", ll_list)
        return ll_list

    @staticmethod
    def expand_list(string, fmt="{:03d}", sep1=",", sep2=":", sep3="-", maxval=None, add_last=False,
                    tstep=None):
        """Expand the lists of forecast start times/lead times.

        Args:
            string (str): _description_
            fmt (str, optional): format. Defaults to "{:03d}".
            sep1 (str, optional): First separator. Defaults to ",".
            sep2 (str, optional): Second separator. Defaults to ":".
            sep3 (str, optional): Third separator. Defaults to "-".
            maxval (int, optional): Maximum value. Defaults to None.
            add_last (bool, optional): if last value should be added. Defaults to False.
            tstep (int, optional): time step in forecast. Needed for minute output.
                                   Defaults to None.

        Raises:
            Exception: _description_
            Exception: _description_
            Exception: _description_

        Returns:
            list: Expaned list of values.

        """
        elements = string.split(sep1)
        expanded_list = []
        if string.strip() == "":
            return expanded_list

        for i in range(0, len(elements)):
            element = elements[i]
            # print(element)
            if element.find(sep2) > 0 or element.find(sep3) > 0:
                step = 1
                if element.find(sep2) > 0:
                    p1, step = element.split(sep2)
                else:
                    p1 = element

                start, end = p1.split(sep3)
                for ltime in range(int(start), int(end) + 1, int(step)):
                    add = True
                    if maxval is not None:
                        if ltime > maxval:
                            add = False
                    if add:
                        if tstep is not None:
                            if (ltime * 60) % tstep == 0:
                                ltime = int(ltime * 60 / tstep)
                            else:
                                logging.critical("Time step %s tstep=%s", str(ltime * tstep),
                                                 str(tstep))
                                raise Exception("Time step is not a minute!")
                        this_ll = fmt.format(ltime)
                        expanded_list.append(this_ll)
            else:
                # print(fmt, element)
                # print(fmt.decode('ascii'))
                add = True
                ltime = int(element)
                if maxval is not None:
                    if ltime > maxval:
                        add = False
                if add:
                    if tstep is not None:
                        if (ltime * 60) % tstep == 0:
                            ltime = int(ltime * 60 / tstep)
                        else:
                            raise Exception("Time step is not a minute! " + str(ltime))
                    ltime = fmt.format(ltime)
                    expanded_list.append(ltime)

        # Add last value if wanted and not existing
        if maxval is not None and add_last:
            if tstep is not None:
                if (maxval * 60) % tstep == 0:
                    maxval = int(maxval * 60 / tstep)
                else:
                    raise Exception("Time step is not a minute!")
            if str(maxval) not in expanded_list:
                ltime = fmt.format(maxval)
                expanded_list.append(ltime)
        return expanded_list

    def expand_hh_and_ll_list(self, hh_list, ll_list, sep=":"):
        """Expand both HH_LIST and LL_LIST.

        Args:
            hh_list (_type_): _description_
            ll_list (_type_): _description_
            sep (str, optional): _description_. Defaults to ":".

        Raises:
            Exception: _description_

        Returns:
            tuple: expanded_hh_list, expanded_ll_list

        """
        # hhs = split_hh_and_ll(hh_list)
        # lls = split_hh_and_ll(ll_list)
        hhs = self.expand_list(hh_list, fmt="{:02d}")
        lls_in = self.expand_list(ll_list, fmt="{:d}")
        # print(hhs)
        # print(lls_in)

        lls = []
        j = 0
        for i in range(0, len(hhs)):
            lls.append(lls_in[j])
            j = j + 1
            if j == len(lls_in):
                j = 0

        if len(hhs) != len(lls):
            raise Exception

        expanded_hh_list = []
        expanded_ll_list = []
        for i in range(0, len(hhs)):
            ll = lls[i]
            # print(i, hhs[i])
            if hhs[i].find(sep) > 0:
                p1, step = hhs[i].split(sep)
                h1, h2 = p1.split("-")
                for hour in range(int(h1), int(h2) + 1, int(step)):
                    hour = f"{hour:02d}"
                    expanded_hh_list.append(hour)
                    expanded_ll_list.append(ll)
            else:
                hour = f"{int(hhs[i]):02d}"
                expanded_hh_list.append(hour)
                expanded_ll_list.append(ll)

        # print(expanded_hh_list, expanded_ll_list)
        return expanded_hh_list, expanded_ll_list


class ConfigurationFromJsonFile(ExpConfiguration):
    """Create an experiment configuration from a json file."""

    def __init__(self, filename):
        """Create an experiment configuration from a json file.

        Args:
            filename (str): Filename

        """
        with open(filename, mode="r", encoding="utf-8") as file_handler:
            all_settings = json.load(file_handler)
        ExpConfiguration.__init__(self, all_settings)
        self.config_file = filename


class Exp(ExpConfiguration):
    """Experiment class."""

    def __init__(self, exp_dependencies, merged_config, stream=None):
        """Instaniate an object of the main experiment class.

        Args:
            exp_dependencies (dict):  Eperiment dependencies
            merged_config (dict): Experiment configuration

        """
        logging.debug("Construct Exp")
        offline_source = exp_dependencies.get("offline_source")
        wdir = exp_dependencies.get("exp_dir")
        exp_name = exp_dependencies.get("exp_name")

        self.name = exp_name
        merged_config["GENERAL"].update({"EXP": exp_name})
        merged_config["GENERAL"].update({"EXP_DIR": wdir})
        self.work_dir = wdir
        self.scripts = exp_dependencies.get("pysurfex_experiment")
        self.pysurfex = exp_dependencies.get("pysurfex")
        self.offline_source = offline_source
        self.config_file = None

        ExpConfiguration.__init__(self, merged_config)

    def dump_exp_configuration(self, filename, indent=None):
        """Dump the exp configuration.

        The configuration has two keys. One for general config and one for member config.

        Args:
            filename (str): filename to dump to
            indent (int, optional): indentation in json file. Defaults to None.
        """
        json.dump(self.settings, open(filename, mode="w", encoding="utf-8"), indent=indent)
        self.config_file = filename


class ExpFromFiles(Exp):
    """Generate Exp object from existing files. Use config files from a setup."""

    def __init__(self, exp_dependencies_file, stream=None):
        """Construct an Exp object from files.

        Args:
            exp_dependencies_file (str): File with exp dependencies

        Raises:
            FileNotFoundError: If file is not found

        """
        logging.debug("Construct ExpFromFiles")
        if os.path.exists(exp_dependencies_file):
            with open(exp_dependencies_file, mode="r", encoding="utf-8") as exp_dependencies_file:
                exp_dependencies = json.load(exp_dependencies_file)
        else:
            raise FileNotFoundError("Experiment dependencies not found " + exp_dependencies_file)

        wdir = exp_dependencies.get("exp_dir")
        self.work_dir = wdir

        # System
        env_system = wdir + "/Env_system"
        if os.path.exists(env_system):
            with open(env_system, mode="r", encoding="utf-8") as env_system:
                env_system = toml.load(env_system)
        else:
            raise FileNotFoundError("System settings not found " + env_system)

        # System file path
        input_paths = wdir + "/Env_input_paths"
        if os.path.exists(input_paths):
            with open(input_paths, mode="r", encoding="utf-8") as input_paths:
                system_file_paths = json.load(input_paths)
        else:
            raise FileNotFoundError("System setting input paths not found " + input_paths)

        # Submission settings
        env_submit = wdir + "/Env_submit"
        if os.path.exists(env_submit):
            with open(env_submit, mode="r", encoding="utf-8") as env_submit:
                env_submit = json.load(env_submit)
        else:
            raise FileNotFoundError("Submision settings not found " + env_submit)

        # Scheduler settings
        env_server = wdir + "/Env_server"
        if os.path.exists(env_server):
            server = scheduler.EcflowServerFromFile(env_server)
        else:
            raise FileNotFoundError("Server settings missing " + env_server)

        # Date/time settings
        # TODO adapt progress object
        dtg = None
        dtgbeg = None
        dtgpp = None
        stream_txt = ""
        if stream is not None:
            stream_txt = f"_stream{stream}_"
        with open(f"{wdir}/progress{stream_txt}.json", mode="r", encoding="utf-8") as progress_file:
            progress = json.load(progress_file)
            dtg = progress["DTG"]
            dtgbeg = progress["DTGBEG"]
        with open(f"{wdir}/progressPP{stream_txt}.json", mode="r", encoding="utf-8") as progress_pp_file:
            progress_pp = json.load(progress_pp_file)
            dtgpp = progress_pp["DTGPP"]
        progress = {
            "DTG": dtg,
            "DTGBEG": dtgbeg,
            "DTGPP": dtgpp
        }

        # Configuration
        config_files_dict = self.get_config_files_dict(wdir)
        all_merged_settings = self.merge_toml_env_from_config_dicts(config_files_dict)

        # Geometry
        domains = wdir + "/config/domains/Harmonie_domains.json"
        if os.path.exists(domains):
            with open(domains, mode="r", encoding="utf-8") as domains:
                domains = json.load(domains)
                all_merged_settings["GEOMETRY"].update({"DOMAINS": domains})
        else:
            raise FileNotFoundError("Domains not found " + domains)

        # Scheduler
        all_merged_settings.update({"SCHEDULER": server.settings})
        # Submission
        all_merged_settings.update({"SUBMISSION": env_submit})
        # System path variables
        all_merged_settings.update({"SYSTEM_FILE_PATHS": system_file_paths})
        # System settings
        all_merged_settings.update({"SYSTEM_VARS": env_system})
        # Date/time
        all_merged_settings.update({"PROGRESS": progress})

        # Troika
        all_merged_settings.update({"TROIKA": { "CONFIG": wdir + "/config/troika_config.yml"}})

        # m_config, member_m_config = experiment_setup.process_merged_settings(all_merged_settings)

        Exp.__init__(self, exp_dependencies, all_merged_settings, stream=stream)

    @staticmethod
    def merge_toml_env_from_config_dicts(config_files):
        """Merge the settings in a config dict.

        Args:
            config_files (list): _description_

        Returns:
            _type_: _description_

        """
        logging.debug("config_files: %s", str(config_files))
        merged_env = {}
        for f in config_files:
            # print(f)
            modification = config_files[f]["toml"]
            merged_env = ExpConfiguration.merge_toml_env(merged_env, modification)
        return merged_env


    @staticmethod
    def get_config_files_dict(work_dir, pysurfex_experiment=None, pysurfex=None, must_exists=True):
        """Get the needed set of configurations files.

        Raises:
            FileNotFoundError: if no config file is found.

        Returns:
            dict: config_files_dict
        """
        # Check existence of needed config files
        # config_file = work_dir + "/config/config.toml"
        # config = toml.load(open(config_file, mode="r", encoding="UTF-8"))
        config = ExpFromFiles.get_config(work_dir, pysurfex_experiment=pysurfex_experiment,
                                         must_exists=must_exists)
        c_files = config["config_files"]
        config_files_dict = {}
        for fname in c_files:
            lfile = work_dir + "/config/" + fname
            if fname == "config_exp_surfex.toml":
                if pysurfex is not None:
                    lfile = pysurfex + "/surfex/cfg/" + fname

            if os.path.exists(lfile):
                print("lfile", lfile)
                toml_dict = toml.load(open(lfile, mode="r", encoding="UTF-8"))
            else:
                raise FileNotFoundError("No config file found for " + lfile)

            config_files_dict.update({
                fname: {
                    "toml": toml_dict,
                    "blocks": config[fname]["blocks"]
                }
            })
        return config_files_dict

    @staticmethod
    def get_config(wdir, pysurfex_experiment=None, must_exists=True):
        """Get the config definiton.

        Args:
            wdir (str): work directory to search for config
            pysurfex_experiment (str, optional): path to pysurfex-experiment
            must_exists (bool, optional): raise exception on not existing. Defaults to True.

        Raises:
            Exception: _description_

        Returns:
            list: List of config files
        """
        # Check existence of needed config files
        local_config = wdir + "/config/config.toml"
        search_configs = [local_config]
        if pysurfex_experiment is not None:
            search_configs.append(pysurfex_experiment + "/config/config.toml")

        c_files = None
        for conf in search_configs:
            if c_files is None and os.path.exists(conf):
                with open(conf, mode="r", encoding="utf-8") as file_handler:
                    c_files = toml.load(file_handler)

        if must_exists and c_files is None:
            raise Exception(f"No config found in {str(search_configs)}")

        return c_files
