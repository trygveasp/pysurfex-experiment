import surfex
import os
import json
import numpy as np
import yaml
from datetime import timedelta, datetime
import shutil


class AbstractTask(object):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        """ Initialize a task run by the default ecflow container

        All tasks implelementing this base class will work with the default ecflow container

        Args:
            task: A scheduler.EcflowTask object
            config(dict): Dict with configuration

            system (dict): System variables for each host number.
            exp_file_paths (dict): Paths to dependencies used in the experiment
            progress (dict): Date/time information for the experiment
            mbr (str): Ensemble member number
            stream (str): Stream number
            debug (bool): Enable debug information
            **kwargs: Arbitrary keyword arguments.

        Raises:

        Returns:

        """

        if surfex is None:
            raise Exception("Surfex module not properly loaded!")

        self.dtg = datetime.strptime(progress["DTG"], "%Y%m%d%H")
        self.dtgbeg = datetime.strptime(progress["DTGBEG"], "%Y%m%d%H")

        self.exp_file_paths = surfex.SystemFilePaths(exp_file_paths)
        self.wd = self.exp_file_paths.get_system_path("exp_dir")

        self.members = None
        self.mbr = mbr
        self.stream = stream
        self.sfx_exp_vars = None
        self.debug = debug
        if self.debug:
            print("        config: ", json.dumps(config, sort_keys=True, indent=2))
            print("        system: ", json.dumps(system, sort_keys=True, indent=2))
            print("exp_file_paths: ", json.dumps(self.exp_file_paths.system_file_paths, sort_keys=True, indent=2))

        # Domain/geo
        self.config = surfex.ConfigurationFromJson(config.copy())

        settings = config["config"]
        self.members = None
        if "FORECAST" in settings:
            if "ENSMSEL" in settings["FORECAST"]:
                self.members = settings["FORECAST"]["ENSMSEL"]
                if len(self.members) == 0:
                    self.members = None
                else:
                    settings = config["member_config"]
        self.settings = settings

        domain = self.get_setting("GEOMETRY#DOMAIN", mbr=self.mbr)
        domains = self.wd + "/config/domains/Harmonie_domains.json"
        domains = json.load(open(domains, "r"))
        domain_json = surfex.set_domain(domains, domain, hm_mode=True)
        geo = surfex.get_geo_object(domain_json, debug=debug)
        # self.config.settings["GEOMETRY"].update({"GEO": geo})
        self.geo = geo

        self.task = task

        wrapper = ""
        if wrapper in kwargs:
            wrapper = kwargs["wrapper"]
        self.wrapper = wrapper

        masterodb = False
        lfagmap = self.get_setting("SURFEX#IO#LFAGMAP", mbr=self.mbr)
        self.csurf_filetype = self.get_setting("SURFEX#IO#CSURF_FILETYPE", mbr=self.mbr)
        self.suffix = surfex.SurfFileTypeExtension(self.csurf_filetype, lfagmap=lfagmap, masterodb=masterodb).suffix

        self.wrk = self.exp_file_paths.get_system_path("wrk_dir", default_dir="default_wrk_dir", mbr=self.mbr,
                                                       basedtg=self.dtg)
        self.archive = self.exp_file_paths.get_system_path("archive_dir", default_dir="default_archive_dir",
                                                           mbr=self.mbr, basedtg=self.dtg)
        os.makedirs(self.archive, exist_ok=True)
        self.bindir = self.exp_file_paths.get_system_path("bin_dir", default_dir="default_bin_dir")

        self.extrarch = self.exp_file_paths.get_system_path("extrarch_dir", default_dir="default_extrarch_dir",
                                                            mbr=self.mbr,  basedtg=self.dtg)
        os.makedirs(self.extrarch, exist_ok=True)
        self.obsdir = self.exp_file_paths.get_system_path("obs_dir", default_dir="default_obs_dir", mbr=self.mbr,
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

        hh = self.dtg.strftime("%H")
        self.fcint = self.get_fcint(hh, mbr=self.mbr)
        self.fg_dtg = self.dtg - timedelta(hours=self.fcint)
        self.next_dtg = self.dtg + timedelta(hours=self.fcint)
        self.next_dtgpp = self.next_dtg
        self.input_path = self.wd + "/nam"

        self.fg_guess_sfx = self.wrk + "/first_guess_sfx"
        self.fc_start_sfx = self.wrk + "/fc_start_sfx"

        self.translation = {
            "t2m": "air_temperature_2m",
            "rh2m": "relative_humidity_2m",
            "sd": "surface_snow_thickness"
        }
        self.sfx_exp_vars = {}
        self.system = system
        if self.system is not None:
            for key in self.system:
                value = self.system[key]
                self.sfx_exp_vars.update({key: value})

    def run(self):

        self.execute()
        self.postfix()

    def execute(self):
        print("WARNING: Using empty base class execute ")

    def postfix(self):
        print("Base class postfix")
        if self.wrk is not None:
            os.chdir(self.wrk)

        if self.wdir is not None:
            shutil.rmtree(self.wdir)

    def get_setting(self, setting, **kwargs):
        """

        Args:
            setting:
            **kwargs:

        Returns:
            this_setting
        """

        check_parsing = False
        if "check_parsing" in kwargs:
            check_parsing = kwargs["check_parsing"]
        kwargs.update({"check_parsing": False})
        this_setting = self.config.get_setting(setting, **kwargs)

        # Parse setting
        kwargs.update({"check_parsing": check_parsing})
        this_setting = self.parse_setting(this_setting, **kwargs)
        return this_setting

    def parse_setting(self, setting, **kwargs):

        verbosity = 0
        if "verbosity" in kwargs:
            verbosity = kwargs["verbosity"]

        check_parsing = True
        if "check_parsing" in kwargs:
            check_parsing = kwargs["check_parsing"]
        # Check on arguments
        if kwargs is not None and isinstance(setting, str):
            validtime = None
            if "validtime" in kwargs:
                validtime = kwargs["validtime"]
            mbr = None
            if "mbr" in kwargs:
                mbr = kwargs["mbr"]
            basedtg = None
            if "basedtg" in kwargs:
                basedtg = kwargs["basedtg"]
            tstep = None
            if "tstep" in kwargs:
                tstep = kwargs["tstep"]
            pert = None
            if "pert" in kwargs:
                pert = kwargs["pert"]
            var = None
            if "var" in kwargs:
                var = kwargs["var"]

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

            if mbr is not None:
                setting = str(setting).replace("@E@", "mbr{:d}".format(int(mbr)))
                setting = str(setting).replace("@EE@", "mbr{:02d}".format(int(mbr)))
                setting = str(setting).replace("@EEE@", "mbr{:03d}".format(int(mbr)))
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
                if verbosity > 2:
                    print(self.sfx_exp_vars, setting)
                for sfx_exp_var in self.sfx_exp_vars:
                    if isinstance(self.sfx_exp_vars[sfx_exp_var], str):
                        if verbosity > 4:
                            print(str(setting), "  <--> ", "@" + sfx_exp_var + "@", self.sfx_exp_vars[sfx_exp_var])
                        setting = str(setting).replace("@" + sfx_exp_var + "@", self.sfx_exp_vars[sfx_exp_var])

        if check_parsing:
            if isinstance(setting, str) and setting.count("@") > 1:
                raise Exception("Setting was not substituted properly? " + setting)

        return setting

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

class Dummy(object):

    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        self.task = task
        print("Dummy task initialized: ", task)
        print("           mbr: ", mbr)
        print("        stream: ", stream)
        print("         debug: ", debug)
        print("        kwargs: ", kwargs)
        print("        Config: ", json.dumps(config, sort_keys=True, indent=2))
        print("        system: ", json.dumps(system, sort_keys=True, indent=2))
        print("exp_file_paths: ", json.dumps(exp_file_paths, sort_keys=True, indent=2))
        print("      progress: ", json.dumps(progress, sort_keys=True, indent=2))

    def run(self):
        print("Dummy task ", self.task, "is run")


class PrepareCycle(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):

        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)

    def run(self):
        self.execute()

    def execute(self):
        if os.path.exists(self.wrk):
            shutil.rmtree(self.wrk)


class QualityControl(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress,  mbr=mbr,
                              stream=stream, debug=debug, **kwargs)
        self.var_name = task.family1

    def execute(self):

        an_time = self.dtg
        # archive_root = self.get_setting("archive_root")
        settings_var = {
          "t2m": {
            "sets": {
              "netatmo": {
                "varname": "Temperature",
                "filetype": "netatmo",
                "tests": {
                  "nometa": {
                    "do_test": True
                  },
                  "domain": {
                    "do_test": True
                  },
                  "blacklist": {
                    "do_test": True
                  },
                  "redundancy": {
                    "do_test": True
                  },
                  "plausibility": {
                    "do_test": True,
                    "maxval": 340,
                    "minval": 200
                  }
                }
              }
            }
          },
          "rh2m": {
            "sets": {
              "netatmo": {
                "varname": "Humidity",
                "filetype": "netatmo",
                "tests": {
                  "nometa": {
                    "do_test": True
                  },
                  "domain": {
                    "do_test": True
                  },
                  "blacklist": {
                    "do_test": True
                  },
                  "redundancy": {
                    "do_test": True
                  },
                  "plausibility": {
                    "do_test": True,
                    "minval": 0,
                    "maxval": 100
                  }
                }
              }
            }
          },
          "sd": {
            "sets": {
              "bufr": {
                "filetype": "bufr",
                "varname": "totalSnowDepth",
                "tests": {
                  "nometa": {
                    "do_test": True
                  },
                  "domain": {
                    "do_test": True
                  },
                  "blacklist": {
                    "do_test": True
                  },
                  "redundancy": {
                    "do_test": True
                  },
                  "plausibility": {
                    "do_test": True,
                    "minval": 0,
                    "maxval": 10000
                  }
                }
              }
            }
          }
        }

        settings = settings_var[self.var_name]
        sfx_lib = self.exp_file_paths.get_system_path("sfx_exp_lib")
        settings.update({"domain": {"domain_file": sfx_lib + "/domain.json"}})
        fg_file = self.exp_file_paths.get_system_file("archive_dir", "raw.nc", basedtg=self.dtg,
                                                      default_dir="default_archive_dir")
        settings.update({
            "firstguess": {
                "fg_file": fg_file,
                "fg_var": self.translation[self.var_name]
            }
        })

        print(self.obsdir)
        output = self.obsdir + "/qc_" + self.translation[self.var_name] + ".json"
        try:
            tests = self.get_setting("OBSERVATIONS#QC#" + self.var_name.upper() + "#TESTS")
        except Exception as e:
            if self.debug:
                print("Use default test " + str(e))
            tests = self.get_setting("OBSERVATIONS#QC#TESTS")

        indent = 2
        blacklist = {}
        print(surfex.__file__)
        tests = surfex.titan.define_quality_control(tests, settings, an_time, domain_geo=self.geo,
                                                    debug=self.debug, blacklist=blacklist)

        if "netatmo" in settings["sets"]:
            filepattern = self.get_setting("OBSERVATIONS#NETATMO_FILEPATTERN")
            settings["sets"]["netatmo"].update({"filepattern": filepattern})
            print(filepattern)
        if "bufr" in settings["sets"]:
            settings["sets"]["bufr"].update({"filepattern": self.obsdir + "/ob@YYYY@@MM@@DD@@HH@"})

        datasources = surfex.obs.get_datasources(an_time, settings["sets"])
        data_set = surfex.TitanDataSet(self.var_name, settings, tests, datasources, an_time, debug=self.debug)
        data_set.perform_tests()

        data_set.write_output(output, indent=indent)


class OptimalInterpolation(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress,  mbr=mbr,
                              stream=stream, debug=debug, **kwargs)
        self.var_name = task.family1

    def execute(self):

        if self.var_name in self.translation:
            var = self.translation[self.var_name]
        else:
            raise Exception

        hlength = 30000
        vlength = 100000
        wlength = 0.5
        max_locations = 20
        elev_gradient = 0
        epsilon = 0.25

        hlength = self.get_setting("OBSERVATIONS#OI#" + self.var_name.upper() + "#HLENGTH", default=hlength)
        vlength = self.get_setting("OBSERVATIONS#OI#" + self.var_name.upper() + "#VLENGTH", default=vlength)
        wlength = self.get_setting("OBSERVATIONS#OI#" + self.var_name.upper() + "#WLENGTH", default=wlength)
        elev_gradient = self.get_setting("OBSERVATIONS#OI#" + self.var_name.upper() + "#GRADIENT",
                                                default=elev_gradient)
        max_locations = self.get_setting("OBSERVATIONS#OI#" + self.var_name.upper() + "#MAX_LOCATIONS",
                                                default=max_locations)
        epsilon = self.get_setting("OBSERVATIONS#OI#" + self.var_name.upper() + "#EPISLON", default=epsilon)
        minvalue = self.get_setting("OBSERVATIONS#OI#" + self.var_name.upper() + "#MINVALUE", default=None,
                                           abort=False)
        maxvalue = self.get_setting("OBSERVATIONS#OI#" + self.var_name.upper() + "#MAXVALUE", default=None,
                                           abort=False)
        input_file = self.archive + "/raw_" + var + ".nc"
        output_file = self.archive + "/an_" + var + ".nc"

        # Get input fields
        geo, validtime, background, glafs, gelevs = surfex.read_first_guess_netcdf_file(input_file, var)

        an_time = validtime
        # Read OK observations
        obs_file = self.exp_file_paths.get_system_file("obs_dir", "qc_" + var + ".json", basedtg=self.dtg,
                                                       default_dir="default_obs_dir")
        observations = surfex.dataset_from_file(an_time, obs_file, qc_flag=0)

        field = surfex.horizontal_oi(geo, background, observations, gelevs=gelevs,
                                     hlength=hlength, vlength=vlength, wlength=wlength,
                                     max_locations=max_locations, elev_gradient=elev_gradient,
                                     epsilon=epsilon, minvalue=minvalue, maxvalue=maxvalue)

        if os.path.exists(output_file):
            os.unlink(output_file)
        surfex.write_analysis_netcdf_file(output_file, field, var, validtime, gelevs, glafs, new_file=True, geo=geo)


class FirstGuess(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)
        self.var_name = task.family1

    def execute(self, **kwargs):

        firstguess = self.get_setting("SURFEX#IO#CSURFFILE") + self.suffix
        fg_file = self.exp_file_paths.get_system_file("first_guess_dir", firstguess, basedtg=self.fg_dtg,
                                                      validtime=self.dtg, default_dir="default_first_guess_dir")

        if os.path.islink(self.fg_guess_sfx):
            os.unlink(self.fg_guess_sfx)
        os.symlink(fg_file, self.fg_guess_sfx)


class CycleFirstGuess(FirstGuess):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        FirstGuess.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                            stream=stream, debug=debug, **kwargs)

    def execute(self):

        firstguess = self.get_setting("SURFEX#IO#CSURFFILE") + self.suffix
        fg_file = self.exp_file_paths.get_system_file("first_guess_dir", firstguess, basedtg=self.fg_dtg,
                                                      validtime=self.dtg, default_dir="default_first_guess_dir")

        if os.path.islink(self.fc_start_sfx):
            os.unlink(self.fc_start_sfx)
        os.symlink(fg_file, self.fc_start_sfx)


class Oi2soda(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)
        self.var_name = task.family1

    def execute(self):

        yy = self.dtg.strftime("%y")
        mm = self.dtg.strftime("%m")
        dd = self.dtg.strftime("%d")
        hh = self.dtg.strftime("%H")
        obfile = "OBSERVATIONS_" + yy + mm + dd + "H" + hh + ".DAT"
        output = self.exp_file_paths.get_system_file("obs_dir", obfile, mbr=self.mbr, basedtg=self.dtg,
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
                if obs_types[ivar] == "T2M":
                    an_variables.update({"t2m": True})
                elif obs_types[ivar] == "RH2M":
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

        surfex.oi2soda(self.dtg, t2m=t2m, rh2m=rh2m, sd=sd, output=output)
        # surfex.run_surfex_binary(binary)


class Qc2obsmon(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)
        self.var_name = task.family1

    def execute(self):

        outdir = self.extrarch + "/ecma_sfc/" + self.dtg.strftime("%Y%m%d%H") + "/"
        os.makedirs(outdir, exist_ok=True)
        output = outdir + "/ecma.db"

        if os.path.exists(output):
            os.unlink(output)
        nnco = self.get_setting("SURFEX#ASSIM#OBS#NNCO")
        obs_types = self.get_setting("SURFEX#ASSIM#OBS#COBS_M")
        for ivar in range(0, len(nnco)):
            if nnco[ivar] == 1:
                if len(obs_types) > ivar:
                    if obs_types[ivar] == "T2M":
                        var_in = "t2m"
                    elif obs_types[ivar] == "RH2M":
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
                        surfex.write_obsmon_sqlite_file(dtg=self.dtg, output=output, qc=qc, fg_file=fg_file,
                                                        an_file=an_file, varname=var_in, file_var=var_name)


class FirstGuess4OI(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)
        self.var_name = task.family1

    def execute(self):

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

        fg = None
        for var in variables:
            try:
                identifier = "INITIAL_CONDITIONS#FG4OI#" + var + "#"
                inputfile = self.get_setting(identifier + "INPUTFILE", basedtg=self.fg_dtg, validtime=self.dtg)
            except Exception as e:
                identifier = "INITIAL_CONDITIONS#FG4OI#"
                inputfile = self.get_setting(identifier + "INPUTFILE", basedtg=self.fg_dtg, validtime=self.dtg)
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

            print(inputfile, fileformat, converter)
            config_file = self.wd + "/config/first_guess.yml"
            config = yaml.load(open(config_file, "r"))
            defs = config[fileformat]
            defs.update({"filepattern": inputfile})

            converter_conf = config[var][fileformat]["converter"]
            if converter not in config[var][fileformat]["converter"]:
                raise Exception("No converter " + converter + " definition found in " + config_file + "!")

            print(converter)
            converter = surfex.read.Converter(converter, validtime, defs, converter_conf, fileformat, validtime)
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


class LogProgress(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)
        self.var_name = task.family1

    def execute(self):

        stream = self.stream
        st = ""
        if stream is not None and stream != "":
            st = "_stream_" + stream
        progress_file = self.wd + "/progress" + st + ".json"

        # Update progress
        next_dtg = self.next_dtg.strftime("%Y%m%d%H")
        dtgbeg = self.dtgbeg.strftime("%Y%m%d%H")
        progress = {"DTG": next_dtg, "DTGBEG": dtgbeg}
        json.dump(progress, open(progress_file, "w"), indent=2)


class LogProgressPP(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)
        self.var_name = task.family1

    def execute(self):

        stream = self.stream

        st = ""
        if stream is not None and stream != "":
            st = "_stream_" + stream

        progress_pp_file = self.wd + "/progressPP" + st + ".json"

        # Update progress
        next_dtgpp = self.next_dtgpp.strftime("%Y%m%d%H")
        progress = {"DTGPP": next_dtgpp}
        json.dump(progress, open(progress_pp_file, "w"), indent=2)


class PrepareOiSoilInput(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)

    def execute(self):
        # Create FG
        raise NotImplementedError


class PrepareOiClimate(AbstractTask):
    def __init__(self, task,  config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)

    def execute(self):
        # Create CLIMATE.dat
        raise NotImplementedError


class PrepareSST(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)

    def execute(self):
        # Create CLIMATE.dat
        raise NotImplementedError


class PrepareLSM(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)

    def execute(self):

        file = self.archive + "/raw_nc"
        output = self.exp_file_paths.get_system_file("climdir", "LSM.DAT", check_existence=False,
                                                     default_dir="default_climdir")
        fileformat = "netcdf"
        converter = "none"

        surfex.lsm_file_assim(var="land_area_fraction", file=file, fileformat=fileformat, output=output,
                              dtg=self.dtg, geo=self.geo, converter=converter)


