"""Experiment configuration."""
import json
import logging
from datetime import timedelta
import experiment_setup


class ExpConfiguration(object):
    """The main experiment configuration.

    It is based on a merged general configuration and a member specific configuration.
    Contrary to the surfex configuration which is a dict with the actual configuration for the
    given member etc.

    """

    def __init__(self, conf_dict, member_conf_dict):
        """Constuct object used to write experiment config files and create suite definition.

        Args:
            conf_dict (dict):
            member_conf_dict (dict):

        """
        self.settings = conf_dict

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
        fcint_members = {}
        fgint = {}
        fgint_members = {}
        for cycle in cycle_times:
            cycle_delta = timedelta(hours=int(cycle))
            cycle_string = self.format_cycle_string(cycle_delta)
            fcint.update({cycle_string: int(self.set_fcint(cycle_delta).total_seconds())})
            fgint.update({cycle_string: int(self.set_fgint(cycle_delta).total_seconds())})
        if self.members is not None:
            for mbr in self.members:
                cycle_times = self.get_cycle_list(mbr=mbr)
                for cycle in cycle_times:
                    cycle_delta = timedelta(hours=int(cycle))
                    cycle_string = self.format_cycle_string(cycle_delta)
                    secs = int(self.set_fcint(cycle_delta, mbr=mbr).total_seconds())
                    fcint_members.update({cycle_string: secs})
                    secs = int(self.set_fgint(cycle_delta, mbr=mbr).total_seconds())
                    fgint_members.update({cycle_string: secs})
        self.settings["GENERAL"].update({"FCINT": fcint})
        self.settings["GENERAL"].update({"FGINT": fgint})
        logging.debug("fcint %s", str(fcint))
        logging.debug("fcint_members %s", str(fcint_members))
        logging.debug("fgint %s", str(fgint))
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

        settings = self.settings.copy()
        if self.members is not None:
            for member in self.members:
                member_settings = {}
                for setting in self.member_settings:
                    member_setting = self.get_setting(setting, mbr=member)
                    member_settings = experiment_setup.merge_toml_env(member_settings,
                                                                      member_setting)

                settings.update({member: experiment_setup.merge_toml_env(settings,
                                                                         member_settings)})

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
            self.settings = experiment_setup.merge_toml_env(self.settings, dsetting)
        else:
            if self.members is not None and str(mbr) in self.members:
                self.member_settings[str(mbr)] = \
                    experiment_setup.merge_toml_env(self.member_settings[str(mbr)], dsetting)
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


class ExpConfigurationFromDict(ExpConfiguration):
    """Create an experiment configuration from a dict."""

    def __init__(self, all_settings):
        """Constuct an experiment configuration from a dict.

        Contains both general settings and member specific settings.

        Args:
            all_settings (dict): All settings in a dict.

        """
        settings = all_settings["config"]
        member_settings = all_settings["member_config"]
        ExpConfiguration.__init__(self, settings, member_settings)


class ConfigurationFromJsonFile(ExpConfigurationFromDict):
    """Create an experiment configuration from a json file."""

    def __init__(self, filename):
        """Create an experiment configuration from a json file.

        Args:
            filename (str): Filename

        """
        with open(filename, mode="r", encoding="utf-8") as file_handler:
            all_settings = json.load(file_handler)
        ExpConfigurationFromDict.__init__(self, all_settings)
