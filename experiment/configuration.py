"""Experiment configuration."""
import json
import logging
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
        ll_list = self.get_ll_list(mbr=mbr)
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
        sep = "#"
        if "sep" in kwargs:
            sep = kwargs["sep"]
        abort = True
        if "abort" in kwargs:
            abort = kwargs["abort"]
        default = None
        if "default" in kwargs:
            default = kwargs["default"]
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
            else:
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

    def get_total_unique_hh_list(self):
        """Get a list of unique start times for the forecasts.

        Returns:
            list: List with times
        """
        # Create a list of all unique HHs from all members
        # print(self.members, self.get_hh_list())
        hh_list_all = []
        if self.members is not None:
            for mbr in self.members:
                hh_l = self.get_hh_list(mbr=mbr)
                for hh in hh_l:
                    hh = "{:02d}".format(int(hh))
                    if hh not in hh_list_all:
                        hh_list_all.append(hh)
        else:
            hh_l = self.get_hh_list()
            for hh in hh_l:
                hh = "{:02d}".format(int(hh))
                if hh not in hh_list_all:
                    hh_list_all.append(hh)

        # print(hh_list_all)
        # Sort this list
        hh_list = []
        for hh in sorted(hh_list_all):
            hh_list.append(hh)

        return hh_list

    def get_fcint(self, cycle, mbr=None):
        """Get the the interval between the forecasts.

        Args:
            cycle (_type_): _description_
            mbr (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        hh_list = self.get_hh_list(mbr=mbr)
        fcint = None
        if len(hh_list) > 1:
            for hh in range(0, len(hh_list)):
                h = int(hh_list[hh]) % 24
                if h == int(cycle) % 24:
                    if hh == 0:
                        fcint = (int(hh_list[0]) - int(hh_list[len(hh_list) - 1])) % 24
                    else:
                        fcint = int(hh_list[hh]) - int(hh_list[hh - 1])
        else:
            fcint = 24
        return fcint

    def get_hh_list(self, mbr=None):
        """Get a list of forecast start times.

        Args:
            mbr (int, optional): ensemble member. Defaults to None.

        Returns:
            list: Forecast start times.

        """
        hh_list = self.get_setting("GENERAL#HH_LIST", mbr=mbr)
        ll_list = self.get_setting("GENERAL#LL_LIST", mbr=mbr)
        hh_list, ll_list = self.expand_hh_and_ll_list(hh_list, ll_list)
        return hh_list

    def get_ll_list(self, mbr=None):
        """Get a list of forecast lead times.

        Args:
            mbr (int, optional): ensemble member. Defaults to None.

        Returns:
            list: Forecast lead times.

        """
        hh_list = self.get_setting("GENERAL#HH_LIST", mbr=mbr)
        ll_list = self.get_setting("GENERAL#LL_LIST", mbr=mbr)
        hh_list, ll_list = self.expand_hh_and_ll_list(hh_list, ll_list)
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
                for ll in range(int(start), int(end) + 1, int(step)):
                    add = True
                    if maxval is not None:
                        if ll > maxval:
                            add = False
                    if add:
                        if tstep is not None:
                            if (ll * 60) % tstep == 0:
                                ll = int(ll * 60 / tstep)
                            else:
                                print(ll)
                                raise Exception("Time step is not a minute!")
                        this_ll = fmt.format(ll)
                        expanded_list.append(this_ll)
            else:
                # print(fmt, element)
                # print(fmt.decode('ascii'))
                add = True
                ll = int(element)
                if maxval is not None:
                    if ll > maxval:
                        add = False
                if add:
                    if tstep is not None:
                        if (ll * 60) % tstep == 0:
                            ll = int(ll * 60 / tstep)
                        else:
                            raise Exception("Time step is not a minute! " + str(ll))
                    ll = fmt.format(ll)
                    expanded_list.append(ll)

        # Add last value if wanted and not existing
        if maxval is not None and add_last:
            if tstep is not None:
                if (maxval * 60) % tstep == 0:
                    maxval = int(maxval * 60 / tstep)
                else:
                    raise Exception("Time step is not a minute!")
            if str(maxval) not in expanded_list:
                ll = fmt.format(maxval)
                expanded_list.append(ll)
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
                for h in range(int(h1), int(h2) + 1, int(step)):
                    hh = "{:02d}".format(h)
                    expanded_hh_list.append(hh)
                    expanded_ll_list.append(ll)
            else:
                hh = "{:02d}".format(int(hhs[i]))
                expanded_hh_list.append(hh)
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
