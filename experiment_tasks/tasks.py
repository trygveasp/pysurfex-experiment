"""General task module."""
from datetime import timedelta, datetime
import os
import json
import shutil
import numpy as np
import yaml
import logging
import surfex


class AbstractTask(object):
    """General abstract task to be implemented by all tasks using default container."""

    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        """Initialize a task run by the default ecflow container.

        All tasks implelementing this base class will work with the default ecflow container

        Args:
            task: A scheduler.EcflowTask object
            config(dict): Dict with configuration
            system (dict): System variables for each host number.
            exp_file_paths (dict): Paths to dependencies used in the experiment
            progress (dict): Date/time information for the experiment
            **kwargs: Arbitrary keyword arguments.

        """
        if surfex is None:
            raise Exception("Surfex module not properly loaded!")

        self.dtg = datetime.strptime(progress["DTG"], "%Y%m%d%H")
        self.dtgbeg = datetime.strptime(progress["DTGBEG"], "%Y%m%d%H")

        self.exp_file_paths = surfex.SystemFilePaths(exp_file_paths)
        self.work_dir = self.exp_file_paths.get_system_path("exp_dir")
        self.lib = self.exp_file_paths.get_system_path("sfx_exp_lib")
        self.stream = config["GENERAL"].get("STREAM")
        self.sfx_exp_vars = None
        logging.debug("        config: %s", json.dumps(config, sort_keys=True, indent=2))
        logging.debug("        system: %s", json.dumps(system, sort_keys=True, indent=2))
        logging.debug("exp_file_paths: %s", json.dumps(self.exp_file_paths.system_file_paths, 
                                                       sort_keys=True, indent=2))

        self.config = surfex.Configuration(config.copy())

        settings = config
        self.mbr = None
        self.members = None
        if "FORECAST" in settings:
            members = settings["FORECAST"].get("ENSMSEL")
            if members is not None:
                self.members = len(members)
            self.mbr = settings["FORECAST"].get("ENSMBR")

        # Domain/geo
        domain = self.get_setting("GEOMETRY#DOMAIN")
        domains = self.get_setting("GEOMETRY#DOMAINS")
        domain_json = surfex.set_domain(domains, domain, hm_mode=True)
        geo = surfex.get_geo_object(domain_json)
        self.geo = geo

        self.task = task

        wrapper = kwargs.get("wrapper")
        if wrapper is None:
            wrapper = ""
        self.wrapper = wrapper

        masterodb = False
        lfagmap = self.get_setting("SURFEX#IO#LFAGMAP")
        self.csurf_filetype = self.get_setting("SURFEX#IO#CSURF_FILETYPE")
        self.suffix = surfex.SurfFileTypeExtension(self.csurf_filetype, lfagmap=lfagmap,
                                                   masterodb=masterodb).suffix

        self.wrk = self.get_system_path("wrk_dir", default_dir="default_wrk_dir", basedtg=self.dtg)
        self.archive = self.get_system_path("archive_dir", default_dir="default_archive_dir",
                                            basedtg=self.dtg)                                         
        os.makedirs(self.archive, exist_ok=True)
        self.bindir = self.get_system_path("bin_dir", default_dir="default_bin_dir")

        self.extrarch = self.get_system_path("extrarch_dir", default_dir="default_extrarch_dir",
                                             basedtg=self.dtg)
        os.makedirs(self.extrarch, exist_ok=True)
        self.obsdir = self.get_system_path("obs_dir", default_dir="default_obs_dir",
                                           basedtg=self.dtg)

        self.exp_file_paths.add_system_file_path("wrk_dir", self.wrk)
        self.exp_file_paths.add_system_file_path("bin_dir", self.bindir)
        self.exp_file_paths.add_system_file_path("archive_dir", self.archive)
        self.exp_file_paths.add_system_file_path("extrarch_dir", self.extrarch)
        self.exp_file_paths.add_system_file_path("obs_dir", self.obsdir)

        os.makedirs(self.obsdir, exist_ok=True)
        self.wdir = str(os.getpid())
        self.wdir = self.wrk + "/" + self.wdir
        print("WDIR=" + self.wdir)
        os.makedirs(self.wdir, exist_ok=True)
        os.chdir(self.wdir)

        hour = self.dtg.strftime("%H")
        self.fcint = self.get_fcint(hour)
        self.fg_dtg = self.dtg - timedelta(hours=self.fcint)
        self.next_dtg = self.dtg + timedelta(hours=self.fcint)
        self.next_dtgpp = self.next_dtg
        self.first_guess_dir = self.get_system_path("first_guess_dir", 
                                                    default_dir="default_first_guess_dir",
                                                    basedtg=self.fg_dtg)   
        self.input_path = self.lib + "/nam"

        self.fg_guess_sfx = self.wrk + "/first_guess_sfx"
        self.fc_start_sfx = self.wrk + "/fc_start_sfx"

        self.translation = {
            "t2m": "air_temperature_2m",
            "rh2m": "relative_humidity_2m",
            "sd": "surface_snow_thickness"
        }
        self.sfx_exp_vars = {
            "EXP": self.config.get_setting("GENERAL#EXP"),
            "SFX_EXP_LIB": self.exp_file_paths.get_system_path("sfx_exp_lib"),
            "SFX_EXP_DATA": self.exp_file_paths.get_system_path("sfx_exp_data"),
            }

    def run(self):
        """Run task.

        Define run sequence.

        """
        self.execute()
        self.postfix()

    def execute(self):
        """Do nothing for base execute task."""
        logging.warning("Using empty base class execute")

    def postfix(self):
        """Do default postfix.
        
        Default is to clean.

        """
        logging.info("Base class postfix")
        if self.wrk is not None:
            os.chdir(self.wrk)

        if self.wdir is not None:
            shutil.rmtree(self.wdir)

    def get_system_path(self, dname, default_dir=None, validtime=None, basedtg=None):
        """Get the system file path.

        Args:
            dname (str): _description_
            default_dir (str, optional): _description_. Defaults to None.
            validtime (_type_, optional): _description_. Defaults to None.
            basedtg (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_

        """
        path = self.exp_file_paths.get_system_path(dname, default_dir=default_dir, mbr=self.mbr,
                                                   validtime=validtime, basedtg=basedtg)
        
        if self.mbr is not None:
            path = str(path).replace("@E@", "mbr{:d}".format(int(self.mbr)))
            path = str(path).replace("@EE@", "mbr{:02d}".format(int(self.mbr)))
            path = str(path).replace("@EEE@", "mbr{:03d}".format(int(self.mbr)))
        else:
            path = str(path).replace("@E@", "")
            path = str(path).replace("@EE@", "")
            path = str(path).replace("@EEE@", "")
        return path

    def get_system_file(self, dname, file, default_dir=None, validtime=None, basedtg=None):
        """Get the system file.

        Args:
            dname (_type_): _description_
            file (_type_): _description_
            default_dir (_type_, optional): _description_. Defaults to None.
            validtime (_type_, optional): _description_. Defaults to None.
            basedtg (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_

        """
        file = self.exp_file_paths.get_system_file(dname, file, default_dir=default_dir,
                                                   mbr=self.mbr,
                                                   validtime=validtime, basedtg=basedtg)
        return file

    def get_setting(self, setting, check_parsing=True, validtime=None, basedtg=None,
                    tstep=None, pert=None, var=None, default=None, abort=True):
        """Get seting.

        Args:
            setting (_type_): _description_
            check_parsing (bool, optional): _description_. Defaults to True.
            validtime (_type_, optional): _description_. Defaults to None.
            basedtg (_type_, optional): _description_. Defaults to None.
            tstep (_type_, optional): _description_. Defaults to None.
            pert (_type_, optional): _description_. Defaults to None.
            var (_type_, optional): _description_. Defaults to None.
            default (_type_, optional): _description_. Defaults to None.
            abort (bool, optional): _description_. Defaults to True.

        Returns:
            _type_: _description_

        """
        this_setting = self.config.get_setting(setting, check_parsing=False, validtime=validtime,
                                               basedtg=basedtg, tstep=tstep, pert=pert, var=var,
                                               default=default, abort=abort)

        # Parse setting
        this_setting = self.parse_setting(this_setting, check_parsing=check_parsing,
                                          validtime=validtime, basedtg=basedtg, tstep=tstep,
                                          pert=pert, var=var)
        return this_setting

    def parse_setting(self, setting, check_parsing=True, validtime=None, basedtg=None, tstep=None,
                     pert=None, var=None):
        """Parse the setting.

        Args:
            setting (_type_): _description_
            check_parsing (bool, optional): _description_. Defaults to True.
            validtime (_type_, optional): _description_. Defaults to None.
            basedtg (_type_, optional): _description_. Defaults to None.
            tstep (_type_, optional): _description_. Defaults to None.
            pert (_type_, optional): _description_. Defaults to None.
            var (_type_, optional): _description_. Defaults to None.

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_

        """
        # Check on arguments
        if isinstance(setting, str):

            if basedtg is not None:
                if isinstance(basedtg, str):
                    basedtg = datetime.strptime(basedtg, "%Y%m%d%H")
            if validtime is not None:
                if isinstance(validtime, str):
                    validtime = datetime.strptime(validtime, "%Y%m%d%H")
            else:
                validtime = basedtg

            if basedtg is not None and validtime is not None:
                lead_time = validtime - basedtg
                setting = str(setting).replace("@YYYY_LL@", validtime.strftime("%Y"))
                setting = str(setting).replace("@MM_LL@", validtime.strftime("%m"))
                setting = str(setting).replace("@DD_LL@", validtime.strftime("%d"))
                setting = str(setting).replace("@HH_LL@", validtime.strftime("%H"))
                setting = str(setting).replace("@mm_LL@", validtime.strftime("%M"))
                lead_seconds = int(lead_time.total_seconds())
                # lead_minutes = int(lead_seconds / 3600)
                lead_hours = int(lead_seconds / 3600)
                setting = str(setting).replace("@LL@", "{:02d}".format(lead_hours))
                setting = str(setting).replace("@LLL@", "{:03d}".format(lead_hours))
                setting = str(setting).replace("@LLLL@", "{:04d}".format(lead_hours))
                if tstep is not None:
                    lead_step = int(lead_seconds / tstep)
                    setting = str(setting).replace("@TTT@", "{:03d}".format(lead_step))
                    setting = str(setting).replace("@TTTT@", "{:04d}".format(lead_step))

            if basedtg is not None:
                setting = str(setting).replace("@YMD@", basedtg.strftime("%Y%m%d"))
                setting = str(setting).replace("@YYYY@", basedtg.strftime("%Y"))
                setting = str(setting).replace("@YY@", basedtg.strftime("%y"))
                setting = str(setting).replace("@MM@", basedtg.strftime("%m"))
                setting = str(setting).replace("@DD@", basedtg.strftime("%d"))
                setting = str(setting).replace("@HH@", basedtg.strftime("%H"))
                setting = str(setting).replace("@mm@", basedtg.strftime("%M"))

            if self.mbr is not None:
                setting = str(setting).replace("@E@", "mbr{:d}".format(int(self.mbr)))
                setting = str(setting).replace("@EE@", "mbr{:02d}".format(int(self.mbr)))
                setting = str(setting).replace("@EEE@", "mbr{:03d}".format(int(self.mbr)))
            else:
                setting = str(setting).replace("@E@", "")
                setting = str(setting).replace("@EE@", "")
                setting = str(setting).replace("@EEE@", "")

            if pert is not None:
                print("replace", pert, "in ", setting)
                setting = str(setting).replace("@PERT@", str(pert))
                print("replaced", pert, "in ", setting)

            if var is not None:
                setting = str(setting).replace("@VAR@", var)

            if self.sfx_exp_vars is not None:
                logging.debug("%s %s", str(self.sfx_exp_vars), str(setting))
                for sfx_exp_var in self.sfx_exp_vars:
                    if isinstance(self.sfx_exp_vars[sfx_exp_var], str):
                        logging.debug("%s  <--> %s @%s@", str(setting), str(sfx_exp_var),
                                                          str(self.sfx_exp_vars[sfx_exp_var]))
                        setting = str(setting).replace("@" + sfx_exp_var + "@",
                                  self.sfx_exp_vars[sfx_exp_var])

        if check_parsing:
            if isinstance(setting, str) and setting.count("@") > 1:
                raise Exception("Setting was not substituted properly? " + setting)

        return setting

    # TODO remove this. Replace by previous and next DTG from ecflow
    def get_fcint(self, cycle):
        """To be removed.

        Args:
            cycle (_type_): _description_

        Returns:
            _type_: _description_

        """
        hh_list = self.get_hh_list()
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

    # TODO remove. Should get a prepared list as input in the configuration.
    def get_hh_list(self):
        """To be removed.

        Returns:
            _type_: _description_

        """
        hh_list = self.get_setting("GENERAL#HH_LIST")
        ll_list = self.get_setting("GENERAL#LL_LIST")
        # print(hh_list, ll_list)
        hh_list, ll_list = self.expand_hh_and_ll_list(hh_list, ll_list)
        return hh_list

    # TODO remove. Should get a prepared list as input in the configuration.
    def get_ll_list(self):
        """To be removed.

        Returns:
            _type_: _description_

        """
        hh_list = self.get_setting("GENERAL#HH_LIST")
        ll_list = self.get_setting("GENERAL#LL_LIST")
        hh_list, ll_list = self.expand_hh_and_ll_list(hh_list, ll_list)
        return ll_list

    # TODO remove. Should get a prepared list as input in the configuration.
    @staticmethod
    def expand_list(string, fmt="{:03d}", sep1=",", sep2=":", sep3="-", maxval=None,
                    add_last=False, tstep=None):
        """To be removed.

        Args:
            string (_type_): _description_
            fmt (str, optional): _description_. Defaults to "{:03d}".
            sep1 (str, optional): _description_. Defaults to ",".
            sep2 (str, optional): _description_. Defaults to ":".
            sep3 (str, optional): _description_. Defaults to "-".
            maxval (_type_, optional): _description_. Defaults to None.
            add_last (bool, optional): _description_. Defaults to False.
            tstep (_type_, optional): _description_. Defaults to None.

        Raises:
            Exception: _description_
            Exception: _description_
            Exception: _description_

        Returns:
            _type_: _description_

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

    # TODO remove. Should get a prepared list as input in the configuration.
    def expand_hh_and_ll_list(self, hh_list, ll_list, sep=":"):
        """To be removed.

        Args:
            hh_list (_type_): _description_
            ll_list (_type_): _description_
            sep (str, optional): _description_. Defaults to ":".

        Raises:
            Exception: _description_

        Returns:
            _type_: _description_

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


class Dummy(object):
    """A dummy task to test the containers.

    Args:
        object (_type_): _description_
    """

    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        """Construct the Dummy task.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_

        """
        self.task = task
        logging.debug("Dummy task initialized: %s", task)
        logging.debug("        kwargs: %s", kwargs)
        logging.debug("        Config: %s", json.dumps(config, sort_keys=True, indent=2))
        logging.debug("        system: %s", json.dumps(system, sort_keys=True, indent=2))
        logging.debug("exp_file_paths: %s", json.dumps(exp_file_paths, sort_keys=True, indent=2))
        logging.debug("      progress: %s", json.dumps(progress, sort_keys=True, indent=2))

    def run(self):
        """Override run."""
        logging.debug("Dummy task %s is run ", self.task)


class PrepareCycle(AbstractTask):
    """Prepare for th cycle to be run.

    Clean up existing directories.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        """Construct the PrepareCycle task.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_

        """
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)

    def run(self):
        """Override run."""
        self.execute()

    def execute(self):
        """Execute."""
        if os.path.exists(self.wrk):
            shutil.rmtree(self.wrk)


class QualityControl(AbstractTask):
    """Perform quakity control of observations.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        """Constuct the QualityControl task.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_

        """
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)
        self.var_name = task.family1

    def execute(self):
        """Execute."""
        an_time = self.dtg

        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib")

        fg_file = self.exp_file_paths.get_system_file("archive_dir", "raw.nc", basedtg=self.dtg,
                                                      default_dir="default_archive_dir")

        # Default
        settings = {
            "domain": {
                "domain_file": sfx_lib + "/domain.json"
            },
            "firstguess": {
                "fg_file": fg_file,
                "fg_var": self.translation[self.var_name]
            }
        }
        default_tests = {
            "nometa": {
                "do_test": True
            },
            "domain": {
                "do_test": True,
            },
            "blacklist": {
                "do_test": True
            },
            "redundancy": {
                "do_test": True
            }
        }

        # T2M
        if self.var_name == "t2m":
            synop_obs = self.get_setting("OBSERVATIONS#SYNOP_OBS_T2M")
            data_sets = {}
            if synop_obs:
                bufr_tests = default_tests
                bufr_tests.update({
                    "plausibility": {
                        "do_test": True,
                        "maxval": 340,
                        "minval": 200
                    }
                })
                filepattern = self.obsdir + "/ob@YYYY@@MM@@DD@@HH@"
                data_sets.update({
                    "bufr": {
                        "filepattern": filepattern,
                        "filetype": "bufr",
                        "varname": "airTemperatureAt2M",
                        "tests": bufr_tests
                    }
                })
            netatmo_obs = self.get_setting("OBSERVATIONS#NETATMO_OBS_T2M")
            if netatmo_obs:
                netatmo_tests = default_tests
                netatmo_tests.update({
                    "sct": {
                        "do_test": True
                    },
                    "plausibility": {
                        "do_test": True,
                        "maxval": 340,
                        "minval": 200
                    }
                })
                filepattern = self.get_setting("OBSERVATIONS#NETATMO_FILEPATTERN",
                                               check_parsing=False)
                data_sets.update({
                    "netatmo": {
                        "filepattern": filepattern,
                        "varname": "Temperature",
                        "filetype": "netatmo",
                        "tests": netatmo_tests
                    }
                })

            settings.update({
                "sets": data_sets
            })

        # RH2M
        elif self.var_name == "rh2m":
            synop_obs = self.get_setting("OBSERVATIONS#SYNOP_OBS_RH2M")
            data_sets = {}
            if synop_obs:
                bufr_tests = default_tests
                bufr_tests.update({
                    "plausibility": {
                        "do_test": True,
                        "maxval": 100,
                        "minval": 0
                    }
                })
                filepattern = self.obsdir + "/ob@YYYY@@MM@@DD@@HH@"
                data_sets.update({
                    "bufr": {
                        "filepattern": filepattern,
                        "filetype": "bufr",
                        "varname": "relativeHumidityAt2M",
                        "tests": bufr_tests
                    }
                })

            netatmo_obs = self.get_setting("OBSERVATIONS#NETATMO_OBS_RH2M")
            if netatmo_obs:
                netatmo_tests = default_tests
                netatmo_tests.update({
                    "sct": {
                        "do_test": True
                    },
                    "plausibility": {
                        "do_test": True,
                        "maxval": 10000,
                        "minval": 0
                    }
                })
                filepattern = self.get_setting("OBSERVATIONS#NETATMO_FILEPATTERN",
                                               check_parsing=False)
                data_sets.update({
                    "netatmo": {
                        "filepattern": filepattern,
                        "varname": "Humidity",
                        "filetype": "netatmo",
                        "tests": netatmo_tests
                    }
                })

            settings.update({
                "sets": data_sets
            })

        # Snow Depth
        elif self.var_name == "sd":
            synop_obs = self.get_setting("OBSERVATIONS#SYNOP_OBS_SD")
            data_sets = {}
            if synop_obs:
                bufr_tests = default_tests
                bufr_tests.update({
                    "plausibility": {
                        "do_test": True,
                        "maxval": 1000,
                        "minval": 0
                    },
                    "firstguess": {
                        "do_test": True,
                        "negdiff": 0.5,
                        "posdiff": 0.5
                    }
                })
                filepattern = self.obsdir + "/ob@YYYY@@MM@@DD@@HH@"
                data_sets.update({
                    "bufr": {
                        "filepattern": filepattern,
                        "filetype": "bufr",
                        "varname": "totalSnowDepth",
                        "tests": bufr_tests
                    }
                })

            settings.update({
                "sets": data_sets
            })
        else:
            raise NotImplementedError

        logging.debug("Settings %s", json.dumps(settings, indent=2, sort_keys=True))

        output = self.obsdir + "/qc_" + self.translation[self.var_name] + ".json"
        uname = self.var_name.upper()
        try:
            tests = self.get_setting(f"OBSERVATIONS#QC#{uname}#TESTS")
        except Exception as e:
            logging.info("Use default test %s", str(e))
            tests = self.get_setting("OBSERVATIONS#QC#TESTS")

        indent = 2
        blacklist = {}
        json.dump(settings, open("settings.json", mode="w", encoding="utf-8"), indent=2)
        tests = surfex.titan.define_quality_control(tests, settings, an_time, domain_geo=self.geo,
                                                    blacklist=blacklist)

        datasources = surfex.obs.get_datasources(an_time, settings["sets"])
        data_set = surfex.TitanDataSet(self.var_name, settings, tests, datasources, an_time)
        data_set.perform_tests()

        logging.debug("Write to %s", output)
        data_set.write_output(output, indent=indent)


class OptimalInterpolation(AbstractTask):
    """Creates a horizontal OI analysis of selected variables.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        """Construct the OptimalInterpolation task.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_

        """
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)
        self.var_name = task.family1

    def execute(self):
        """Execute."""
        if self.var_name in self.translation:
            var = self.translation[self.var_name]
        else:
            raise Exception(f"No translation for {self.var_name}")

        hlength = 30000
        vlength = 100000
        wlength = 0.5
        max_locations = 20
        elev_gradient = 0
        epsilon = 0.25

        uname = self.var_name.upper()
        hlength = self.get_setting(f"OBSERVATIONS#OI#{uname}#HLENGTH", default=hlength)
        vlength = self.get_setting(f"OBSERVATIONS#OI#{uname}#VLENGTH", default=vlength)
        wlength = self.get_setting(f"OBSERVATIONS#OI#{uname}#WLENGTH", default=wlength)
        elev_gradient = self.get_setting(f"OBSERVATIONS#OI#{uname}#GRADIENT", default=elev_gradient)
        max_locations = self.get_setting(f"OBSERVATIONS#OI#{uname}#MAX_LOCATIONS", default=max_locations)
        epsilon = self.get_setting(f"OBSERVATIONS#OI#{uname}#EPSILON", default=epsilon)
        minvalue = self.get_setting(f"OBSERVATIONS#OI#{uname}#MINVALUE", default=None, abort=False)
        maxvalue = self.get_setting(f"OBSERVATIONS#OI#{uname}#MAXVALUE", default=None, abort=False)
        input_file = self.archive + "/raw_" + var + ".nc"
        output_file = self.archive + "/an_" + var + ".nc"

        # Get input fields
        geo, validtime, background, glafs, gelevs = \
                                               surfex.read_first_guess_netcdf_file(input_file, var)

        an_time = validtime
        # Read OK observations
        obs_file = self.exp_file_paths.get_system_file("obs_dir", "qc_" + var + ".json", 
                                                       basedtg=self.dtg,
                                                       default_dir="default_obs_dir")
        observations = surfex.dataset_from_file(an_time, obs_file, qc_flag=0)

        field = surfex.horizontal_oi(geo, background, observations, gelevs=gelevs,
                                     hlength=hlength, vlength=vlength, wlength=wlength,
                                     max_locations=max_locations, elev_gradient=elev_gradient,
                                     epsilon=epsilon, minvalue=minvalue, maxvalue=maxvalue)

        logging.debug("Write output file %s", output_file)
        if os.path.exists(output_file):
            os.unlink(output_file)
        surfex.write_analysis_netcdf_file(output_file, field, var, validtime, gelevs, glafs, 
                                          new_file=True, geo=geo)


class FirstGuess(AbstractTask):
    """Find first guess.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        """Construct a FistGuess task.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_
        """
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)
        self.var_name = task.family1

    def execute(self):
        """Execute."""
        firstguess = self.get_setting("SURFEX#IO#CSURFFILE") + self.suffix
        fg_file = self.exp_file_paths.get_system_file("first_guess_dir", firstguess,
                                                      basedtg=self.fg_dtg,
                                                      validtime=self.dtg,
                                                      default_dir="default_first_guess_dir")

        if os.path.islink(self.fg_guess_sfx) or os.path.exists(self.fg_guess_sfx):
            os.unlink(self.fg_guess_sfx)
        os.symlink(fg_file, self.fg_guess_sfx)


class CycleFirstGuess(FirstGuess):
    """Cycle the first guess.

    Args:
        FirstGuess (_type_): _description_
    """

    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        """Construct the cycled first guess object.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_
        """
        FirstGuess.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)

    def execute(self):
        """Execute."""
        firstguess = self.get_setting("SURFEX#IO#CSURFFILE") + self.suffix
        fg_file = self.exp_file_paths.get_system_file("first_guess_dir", firstguess,
                                                      basedtg=self.fg_dtg,
                                                      validtime=self.dtg,
                                                      default_dir="default_first_guess_dir")

        if os.path.islink(self.fc_start_sfx):
            os.unlink(self.fc_start_sfx)
        os.symlink(fg_file, self.fc_start_sfx)


class Oi2soda(AbstractTask):
    """Convert OI analysis to an ASCII file for SODA.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        """Construct the Oi2soda task.

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_
        """
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)
        self.var_name = task.family1

    def execute(self):
        """Execute."""
        yy = self.dtg.strftime("%y")
        mm = self.dtg.strftime("%m")
        dd = self.dtg.strftime("%d")
        hh = self.dtg.strftime("%H")
        obfile = "OBSERVATIONS_" + yy + mm + dd + "H" + hh + ".DAT"
        output = self.get_system_file("obs_dir", obfile, basedtg=self.dtg,
                                                 default_dir="default_obs_dir")

        t2m = None
        rh2m = None
        sd = None

        an_variables = {"t2m": False, "rh2m": False, "sd": False}
        obs_types = self.get_setting("SURFEX#ASSIM#OBS#COBS_M")
        nnco = self.get_setting("SURFEX#ASSIM#OBS#NNCO")
        snow_ass = self.get_setting("SURFEX#ASSIM#ISBA#UPDATE_SNOW_CYCLES")
        snow_ass_done = False
        if len(snow_ass) > 0:
            hh = int(self.dtg.strftime("%H"))
            for sn in snow_ass:
                if hh == int(sn):
                    snow_ass_done = True

        for ivar in range(0, len(obs_types)):
            if nnco[ivar] == 1:
                if obs_types[ivar] == "T2M" or obs_types[ivar] == "T2M_P":
                    an_variables.update({"t2m": True})
                elif obs_types[ivar] == "HU2M" or obs_types[ivar] == "HU2M_P":
                    an_variables.update({"rh2m": True})
                elif obs_types[ivar] == "SWE":
                    if snow_ass_done:
                        an_variables.update({"sd": True})

        for var in an_variables:
            if an_variables[var]:
                var_name = self.translation[var]
                if var == "t2m":
                    t2m = {
                        "file": self.archive + "/an_" + var_name + ".nc",
                        "var": var_name
                    }
                elif var == "rh2m":
                    rh2m = {
                        "file": self.archive + "/an_" + var_name + ".nc",
                        "var": var_name
                    }
                elif var == "sd":
                    sd = {
                        "file": self.archive + "/an_" + var_name + ".nc",
                        "var": var_name
                    }
        logging.debug("t2m  %s ",t2m)
        logging.debug("rh2m %s", rh2m)
        logging.debug("sd   %s", sd)
        logging.debug("Write to %s", output)
        surfex.oi2soda(self.dtg, t2m=t2m, rh2m=rh2m, sd=sd, output=output)


class Qc2obsmon(AbstractTask):
    """Convert QC data to obsmon SQLite data.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, task, config, system, exp_file_paths, progress,**kwargs):
        """Construct the QC2obsmon data."""
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)
        self.var_name = task.family1

    def execute(self):
        """Execute."""
        outdir = self.extrarch + "/ecma_sfc/" + self.dtg.strftime("%Y%m%d%H") + "/"
        os.makedirs(outdir, exist_ok=True)
        output = outdir + "/ecma.db"

        logging.debug("Write to %s", output)
        if os.path.exists(output):
            os.unlink(output)
        nnco = self.get_setting("SURFEX#ASSIM#OBS#NNCO")
        obs_types = self.get_setting("SURFEX#ASSIM#OBS#COBS_M")
        for ivar in range(0, len(nnco)):
            if nnco[ivar] == 1:
                if len(obs_types) > ivar:
                    if obs_types[ivar] == "T2M" or obs_types[ivar] == "T2M_P":
                        var_in = "t2m"
                    elif obs_types[ivar] == "HU2M" or obs_types[ivar] == "HU2M_P":
                        var_in = "rh2m"
                    elif obs_types[ivar] == "SWE":
                        var_in = "sd"
                    else:
                        raise NotImplementedError(obs_types[ivar])

                    if var_in != "sd":
                        var_name = self.translation[var_in]
                        qc = self.obsdir + "/qc_" + var_name + ".json"
                        fg_file = self.archive + "/raw_" + var_name + ".nc"
                        an_file = self.archive + "/an_" + var_name + ".nc"
                        surfex.write_obsmon_sqlite_file(dtg=self.dtg, output=output, qc=qc, 
                                                        fg_file=fg_file,
                                                        an_file=an_file, varname=var_in,
                                                        file_var=var_name)


class FirstGuess4OI(AbstractTask):
    """Create a first guess to be used for OI.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        """Construct the FirstGuess4OI task."""
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)
        self.var_name = task.family1

    def execute(self):
        """Execute."""
        validtime = self.dtg

        extra = ""
        symlink_files = {}
        if self.var_name in self.translation:
            var = self.translation[self.var_name]
            variables = [var]
            extra = "_" + var
            symlink_files.update({self.archive + "/raw.nc":  "raw" + extra + ".nc"})
        else:
            var_in = []
            nnco = self.get_setting("SURFEX#ASSIM#OBS#NNCO")

            for ivar in range(0, len(nnco)):
                if nnco[ivar] == 1:
                    if ivar == 0:
                        var_in.append("t2m")
                    elif ivar == 1:
                        var_in.append("rh2m")
                    elif ivar == 4:
                        var_in.append("sd")

            variables = []
            try:
                for var in var_in:
                    var_name = self.translation[var]
                    variables.append(var_name)
                    symlink_files.update({self.archive + "/raw_" + var_name + ".nc":  "raw.nc"})
            except ValueError:
                raise Exception("Variables could not be translated")

        variables = variables + ["altitude", "land_area_fraction"]

        output = self.archive + "/raw" + extra + ".nc"
        cache_time = 3600
        # if "cache_time" in kwargs:
        #     cache_time = kwargs["cache_time"]
        cache = surfex.cache.Cache(True, cache_time)
        # cache = None
        if os.path.exists(output):
            print("Output already exists " + output)
        else:
            self.write_file(output, variables, self.geo, validtime, cache=cache)

        # Create symlinks
        for target in symlink_files:
            linkfile = symlink_files[target]
            if os.path.lexists(target):
                os.unlink(target)
            os.symlink(linkfile, target)

    def write_file(self, output, variables, geo, validtime, cache=None):
        """Write the first guess file.

        Args:
            output (_type_): _description_
            variables (_type_): _description_
            geo (_type_): _description_
            validtime (_type_): _description_
            cache (_type_, optional): _description_. Defaults to None.

        Raises:
            Exception: _description_

        """
        fg = None
        for var in variables:
            try:
                identifier = "INITIAL_CONDITIONS#FG4OI#" + var + "#"
                inputfile = self.get_setting(identifier + "INPUTFILE", basedtg=self.fg_dtg, 
                                             validtime=self.dtg)
            except Exception as e:
                identifier = "INITIAL_CONDITIONS#FG4OI#"
                inputfile = self.get_setting(identifier + "INPUTFILE", basedtg=self.fg_dtg,
                                             validtime=self.dtg)
            try:
                identifier = "INITIAL_CONDITIONS#FG4OI#" + var + "#"
                fileformat = self.get_setting(identifier + "FILEFORMAT")
            except Exception as e:
                identifier = "INITIAL_CONDITIONS#FG4OI#"
                fileformat = self.get_setting(identifier + "FILEFORMAT")
            try:
                identifier = "INITIAL_CONDITIONS#FG4OI#" + var + "#"
                converter = self.get_setting(identifier + "CONVERTER")
            except Exception as e:
                identifier = "INITIAL_CONDITIONS#FG4OI#"
                converter = self.get_setting(identifier + "CONVERTER")
            try:
                identifier = "INITIAL_CONDITIONS#FG4OI#" + var + "#"
                input_geo_file = self.get_setting(identifier + "INPUT_GEO_FILE")
            except Exception as e:
                identifier = "INITIAL_CONDITIONS#FG4OI#"
                input_geo_file = self.get_setting(identifier + "INPUT_GEO_FILE")

            print(inputfile, fileformat, converter, input_geo_file)
            config_file = self.lib + "/config/first_guess.yml"
            config = yaml.safe_load(open(config_file, "r"))
            defs = config[fileformat]
            geo_input = None
            if input_geo_file != "":
                geo_iput = surfex.get_geo_object(open(input_geo_file, mode="r", encoding="utf-8"))
            defs.update({
                "filepattern": inputfile,
                "geo_input": geo_input
                })

            converter_conf = config[var][fileformat]["converter"]
            if converter not in config[var][fileformat]["converter"]:
                raise Exception(f"No converter {converter} definition found in {config_file}!")

            fcint_seconds = self.fcint * 3600.
            defs.update({"fcint": fcint_seconds})
            initial_basetime = validtime - timedelta(seconds=fcint_seconds)
            logging.debug("Converter=%s", str(converter))
            logging.debug("Converter_conf=%s", str(converter_conf))
            logging.debug("Defs=%s", defs)
            logging.debug("valitime=%s fcint=%s initial_basetime=%s", str(validtime), 
                          str(fcint_seconds), str(initial_basetime))
            logging.debug("Fileformat: %s", fileformat)

            # converter = surfex.read.Converter(converter, validtime, defs, converter_conf,
            #                                   fileformat, validtime)
            converter = surfex.read.Converter(converter, initial_basetime, defs, converter_conf,
                                              fileformat)
            field = surfex.read.ConvertedInput(geo, var, converter).read_time_step(validtime, cache)
            field = np.reshape(field, [geo.nlons, geo.nlats])

            # Create file
            if fg is None:
                nx = geo.nlons
                ny = geo.nlats
                fg = surfex.create_netcdf_first_guess_template(variables, nx, ny, output)
                fg.variables["time"][:] = float(validtime.strftime("%s"))
                fg.variables["longitude"][:] = np.transpose(geo.lons)
                fg.variables["latitude"][:] = np.transpose(geo.lats)
                fg.variables["x"][:] = [i for i in range(0, nx)]
                fg.variables["y"][:] = [i for i in range(0, ny)]

            if var == "altitude":
                field[field < 0] = 0

            fg.variables[var][:] = np.transpose(field)

        if fg is not None:
            fg.close()


'''
# Not used/tested tasks yet

class PrepareOiSoilInput(AbstractTask):

    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)

    def execute(self):
        # Create FG
        raise NotImplementedError


class PrepareOiClimate(AbstractTask):
    def __init__(self, task,  config, system, exp_file_paths, progress, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)

    def execute(self):
        # Create CLIMATE.dat
        raise NotImplementedError


class PrepareSST(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)

    def execute(self):
        # Create CLIMATE.dat
        raise NotImplementedError


class PrepareLSM(AbstractTask):
    """Prepare land-sea mask.

    Args:
        AbstractTask (_type_): _description_
    """

    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        """_summary_

        Args:
            task (_type_): _description_
            config (_type_): _description_
            system (_type_): _description_
            exp_file_paths (_type_): _description_
            progress (_type_): _description_
        """
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, **kwargs)


    def execute(self):

        file = self.archive + "/raw_nc"
        output = self.exp_file_paths.get_system_file("climdir", "LSM.DAT", check_existence=False,
                                                     default_dir="default_climdir")
        fileformat = "netcdf"
        converter = "none"

        surfex.lsm_file_assim(var="land_area_fraction", file=file, fileformat=fileformat,
                              output=output, dtg=self.dtg, geo=self.geo, converter=converter)
'''
