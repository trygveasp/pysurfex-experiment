import experiment_setup
import json


class Configuration(object):

    def __init__(self, conf_dict, member_conf_dict, geo=None, debug=False):

        self.debug = debug
        self.settings = conf_dict
        if "GEOMETRY" not in self.settings:
            self.settings.update({"GEOMETRY": {}})
        if geo is None:
            geo = "Not set"
        self.settings["GEOMETRY"].update({"GEO": geo})

        # Set default file names
        if "CPGDFILE" not in self.settings["SURFEX"]["IO"]:
            self.settings["SURFEX"]["IO"].update({"CPGDFILE": "PGD"})
        if "CPREPFILE" not in self.settings["SURFEX"]["IO"]:
            self.settings["SURFEX"]["IO"].update({"CPREPFILE": "PREP"})
        if "CSURFFILE" not in self.settings["SURFEX"]["IO"]:
            self.settings["SURFEX"]["IO"].update({"CSURFFILE": "SURFOUT"})
        if "LFAGMAP" not in self.settings["SURFEX"]["IO"]:
            self.settings["SURFEX"]["IO"].update({"LFAGMAP": True})

        self.settings["SURFEX"]["ASSIM"]["ISBA"]["EKF"].update({"FILE_PATTERN": "SURFOUT_PERT@PERT@"})

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
        for p in range(0, len(nncv)):
            if nncv[p] == 1:
                perts.append(p)
        self.perts = perts

    def dump_json(self, filename, indent=None, debug=False):
        if json is None:
            raise Exception("json module not loaded")

        settings = {
            "settings": self.settings,
            "member_settings": self.member_settings
        }
        if debug:
            print(__file__, settings)
        json.dump(open(filename, "w"), settings, indent=indent)

    def max_fc_length(self, mbr=None):
        ll_list = self.get_ll_list(mbr=mbr)
        max_fc = -1
        for ll in ll_list:
            if int(ll) > int(max_fc):
                max_fc = int(ll)
        return max_fc

    @staticmethod
    def has_sfc_analysis(anasurf):
        if anasurf in ["CANARI", "gridpp"]:
            return True
        else:
            return False

    def setting_is(self, setting, value, **kwargs):
        if self.get_setting(setting, **kwargs) == value:
            return True
        else:
            return False

    def setting_is_not(self, setting, value, **kwargs):
        found = False
        if self.get_setting(setting, **kwargs) == value:
            found = True

        if found:
            return False
        else:
            return True

    def value_is_one_of(self, setting, value, **kwargs):
        found = False
        setting = self.get_setting(setting, **kwargs)
        # if type(setting) is not list:
        #    raise Exception("Excpected a list as input, got ", type(setting))
        for s in setting:
            if s == value:
                found = True
        return found

    def value_is_not_one_of(self, setting, value, **kwargs):

        found = self.value_is_one_of(setting, value, **kwargs)
        if found:
            return False
        else:
            return True

    def setting_is_one_of(self, setting, values, **kwargs):
        found = False
        setting = self.get_setting(setting, **kwargs)
        if type(values) is not list:
            raise Exception("Excpected a list as input, got ", type(values))
        for v in values:
            if setting == v:
                found = True
        return found

    def setting_is_not_one_of(self, setting, values, **kwargs):

        found = self.setting_is_one_of(setting, values, **kwargs)
        if found:
            return False
        else:
            return True

    def get_setting(self, setting, **kwargs):
        mbr = None
        if mbr in kwargs:
            mbr = kwargs["mbr"]
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
            # print(this_setting)
            if len(keys) > 1:
                for key in keys[1:]:
                    if key in this_setting:
                        this_setting = this_setting[key]
                        # Time information
                        # this_setting = surfex.SystemFilePaths.substitute_string(this_setting)
                        # this_setting = surfex.SystemFilePaths.parse_setting(this_setting, **kwargs)
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

        # print(setting, this_setting, mbr, type(this_setting))
        return this_setting

    def update_setting(self, setting, value, mbr=None, sep="#"):

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
                self.member_settings[str(mbr)] = experiment_setup.merge_toml_env(self.member_settings[str(mbr)],
                                                                                 dsetting)
            else:
                raise Exception("Not a valid member: " + str(mbr))

    def get_total_unique_hh_list(self):
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
        hh_list = self.get_setting("GENERAL#HH_LIST", mbr=mbr)
        ll_list = self.get_setting("GENERAL#LL_LIST", mbr=mbr)
        # print(hh_list, ll_list)
        hh_list, ll_list = self.expand_hh_and_ll_list(hh_list, ll_list)
        return hh_list

    def get_ll_list(self, mbr=None):
        hh_list = self.get_setting("GENERAL#HH_LIST", mbr=mbr)
        ll_list = self.get_setting("GENERAL#LL_LIST", mbr=mbr)
        hh_list, ll_list = self.expand_hh_and_ll_list(hh_list, ll_list)
        return ll_list

    @staticmethod
    def expand_list(string, fmt="{:03d}", sep1=",", sep2=":", sep3="-", maxval=None, add_last=False, tstep=None):
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


class ConfigurationFromJson(Configuration):

    def __init__(self, all_settings, debug=False):
        if json is None:
            raise Exception("json module not loaded")

        settings = all_settings["config"]
        member_settings = all_settings["member_config"]
        Configuration.__init__(self, settings, member_settings, debug=debug)


class ConfigurationFromJsonFile(Configuration):

    def __init__(self, filename, debug=False):
        if json is None:
            raise Exception("json module not loaded")

        all_settings = json.load(open(filename, "r"))
        print(filename)
        print(all_settings)
        settings = all_settings["config"]
        member_settings = all_settings["member_config"]
        Configuration.__init__(self, settings, member_settings, debug=debug)
