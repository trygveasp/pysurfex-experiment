import os
import json
import toml


class System(object):
    def __init__(self, host_system, exp_name):

        # print(host_system)
        self.system_variables = ["SFX_EXP_DATA", "SFX_EXP_LIB", "JOBOUTDIR", "MKDIR",
                                 "RSYNC", "HOSTS", "LOGIN_HOST", "SCHEDULER_PYTHONPATH", "SYNC_DATA", "SURFEX_CONFIG"]
        self.hosts = None
        self.exp_name = exp_name

        # Set system0 from system_dict
        system0 = {}
        for var in self.system_variables:
            if var == "HOSTS":
                self.hosts = host_system["HOST_SYSTEM"]["HOSTS"]
            elif var == "HOST":
                pass
            else:
                if var in host_system["HOST_SYSTEM"]:
                    system0.update({var: host_system["HOST_SYSTEM"][var]})

                # Always sync for HOST0
                elif var == "SYNC_DATA":
                    pass
                else:
                    raise Exception("Variable is missing: " + var)

        system = {}
        system.update({"HOSTS": self.hosts})
        for host in range(0, len(self.hosts)):
            systemn = system0.copy()
            systemn.update({"HOST": self.hosts[host]})
            hostn = "HOST" + str(host)
            if hostn in host_system["HOST_SYSTEM"]:
                for key in host_system["HOST_SYSTEM"][hostn]:
                    value = host_system["HOST_SYSTEM"][hostn][key]
                    # print(hostn, key, value)
                    systemn.update({key: value})
            system.update({str(host): systemn})

        self.system = system

    def dump_system_vars(self, filename, indent=None, stream=None):

        system_vars = {}
        for host in range(0, len(self.hosts)):
            host = str(host)
            var_host = {}
            for key in self.system_variables:
                if key == "HOSTS":
                    value = self.get_var(key, host, stream=stream)
                    print(value)
                    var_host.update({"HOSTNAME": value[int(host)]})
                else:
                    value = self.get_var(key, host, stream=stream)
                    var_host.update({key: value})
            system_vars.update({host: var_host})
        json.dump(system_vars, open(filename, "w"), indent=indent)

    def get_var(self, var, host, stream=None):
        if var == "HOSTS":
            if self.hosts is not None:
                return self.hosts
            else:
                raise Exception("Variable " + var + " not found in system")
        else:
            if var in self.system[str(host)]:
                if self.system[str(host)][var] is None:
                    raise Exception(var + " is None!")

                if stream is None:
                    stream = ""

                value = self.system[str(host)][var]
                if isinstance(value, str):
                    value = value.replace("@STREAM@", str(stream))
                    value = value.replace("@EXP@", self.exp_name)
                    value = value.replace("@USER@", os.environ["USER"])
                return value
            elif var == "SYNC_DATA" and host == "0":
                pass
            else:
                raise Exception("Variable " + var + " not found in system")


class SystemFromFile(System):
    def __init__(self, env_system_file, exp_name, debug=False):

        # system = System.get_file_name(wdir, full_path=True)
        if debug:
            print(__file__, env_system_file)
        if os.path.exists(env_system_file):
            host_system = toml.load(open(env_system_file, "r"))
        else:
            raise FileNotFoundError(env_system_file)
        System.__init__(self, host_system, exp_name)


class SystemFilePathsFromSystem(object):

    """

    Also set SFX_EXP system variables (File stucture/ssh etc)

    """
    def __init__(self, paths_in, system, hosts=None, stream=None, wdir=None):
        # surfex.SystemFilePaths.__init__(self, paths)
        if hosts is None:
            hosts = ["0"]

        # override paths from system file
        paths = {}
        print(paths)
        for host in range(0, len(hosts)):
            host = str(host)
            paths_host = {}
            paths_host.update(paths_in)
            if wdir is not None:
                paths_host.update({"exp_dir": wdir})

            sfx_data = system.get_var("SFX_EXP_DATA", host=host, stream=stream)
            sfx_lib = system.get_var("SFX_EXP_LIB", host=host, stream=stream)

            default_bin_dir = sfx_data + "/lib/offline/exe/"
            default_clim_dir = sfx_data + "/climate/"
            default_archive_dir = sfx_data + "/archive/@YYYY@/@MM@/@DD@/@HH@/@EEE@/"
            default_extarch_dir = sfx_data + "/archive/extract/"
            default_forcing_dir = sfx_data + "/forcing/@YYYY@@MM@@DD@@HH@/@EEE@/"
            default_obs_dir = sfx_data + "/archive/observations/@YYYY@/@MM@/@DD@/@HH@/@EEE@/"
            first_guess_dir = default_archive_dir
            wrk_dir = sfx_data + "/@YYYY@@MM@@DD@_@HH@/@EEE@/"
            paths_host.update({
                "sfx_exp_data": sfx_data,
                "sfx_exp_lib": sfx_lib,
                "default_bin_dir": default_bin_dir,
                "default_archive_dir": default_archive_dir,
                "default_extrarch_dir": default_extarch_dir,
                "default_climdir": default_clim_dir,
                "default_wrk_dir": wrk_dir,
                "default_forcing_dir": default_forcing_dir,
                "default_pgd_dir": default_clim_dir,
                "default_prep_dir": default_archive_dir,
                "default_obs_dir": default_obs_dir,
                "default_first_guess_dir": first_guess_dir
            })
            paths.update({host: paths_host})
        self.paths = paths

    def dump_system(self, filename, indent=None):
        json.dump(self.paths, open(filename, "w"), indent=indent)


class SystemFilePathsFromSystemFile(SystemFilePathsFromSystem):

    """

    Also set SFX_EXP system variables (File stucture/ssh etc)

    """
    def __init__(self, system_file_paths, system, name, hosts=None, stream=None, wdir=None):
        system_file_paths = json.load(open(system_file_paths, "r"))
        system = SystemFromFile(system, name)
        SystemFilePathsFromSystem.__init__(self, system_file_paths, system, hosts=hosts, stream=stream, wdir=wdir)
