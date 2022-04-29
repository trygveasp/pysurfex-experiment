from experiment_tasks import AbstractTask
import surfex
import os
import yaml
from datetime import timedelta
import json


class Forcing(AbstractTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, config, system, exp_file_paths, progress, mbr=mbr,
                              stream=stream, debug=debug, **kwargs)
        self.var_name = task.family1

        user_config = None
        if "user_config" in kwargs:
            user_config = kwargs["user_config"]
        self.user_config = user_config

    def execute(self):

        dtg = self.dtg
        hh = self.dtg.strftime("%H")
        fcint = self.get_fcint(hh, mbr=self.mbr)

        kwargs = {}
        if self.user_config is not None:
            user_config = yaml.safe_load(open(self.user_config))
            kwargs.update({"user_config": user_config})

        json.dump(self.geo.json, open(self.wdir + "/domain.json", "w"), indent=2)
        kwargs.update({"domain": self.wdir + "/domain.json"})
        global_config = self.wd + "/config/config.yml"
        global_config = yaml.safe_load(open(global_config, "r"))
        kwargs.update({"config": global_config})

        kwargs.update({"dtg_start": dtg.strftime("%Y%m%d%H")})
        kwargs.update({"dtg_stop": (dtg + timedelta(hours=fcint)).strftime("%Y%m%d%H")})

        forcing_dir = self.exp_file_paths.get_system_path("forcing_dir", basedtg=self.dtg,
                                                          default_dir="default_forcing_dir")
        os.makedirs(forcing_dir, exist_ok=True)

        output_format = self.get_setting("SURFEX#IO#CFORCING_FILETYPE").lower()
        if output_format == "netcdf":
            output = forcing_dir + "/FORCING.nc"
        else:
            raise NotImplementedError(output_format)

        kwargs.update({"of": output})
        kwargs.update({"output_format": output_format})

        pattern = self.get_setting("FORCING#PATTERN", check_parsing=False)
        input_format = self.config.get_setting("FORCING#INPUT_FORMAT")
        zref = self.get_setting("FORCING#ZREF")
        zval = self.get_setting("FORCING#ZVAL")
        uref = self.get_setting("FORCING#UREF")
        uval = self.get_setting("FORCING#UVAL")
        zsoro_converter = self.get_setting("FORCING#ZSORO_CONVERTER")
        sca_sw = self.get_setting("FORCING#SCA_SW")
        co2 = self.get_setting("FORCING#CO2")
        rain_converter = self.get_setting("FORCING#RAIN_CONVERTER")
        wind_converter = self.get_setting("FORCING#WIND_CONVERTER")
        wind_dir_converter = self.get_setting("FORCING#WINDDIR_CONVERTER")
        debug = self.get_setting("FORCING#DEBUG")

        kwargs.update({"input_format": input_format})
        kwargs.update({"pattern": pattern})
        kwargs.update({"zref": zref})
        kwargs.update({"zval": zval})
        kwargs.update({"uref": uref})
        kwargs.update({"uval": uval})
        kwargs.update({"zsoro_converter": zsoro_converter})
        kwargs.update({"sca_sw": sca_sw})
        kwargs.update({"co2": co2})
        kwargs.update({"rain_converter": rain_converter})
        kwargs.update({"wind_converter": wind_converter})
        kwargs.update({"wind_dir_converter": wind_dir_converter})
        kwargs.update({"debug": debug})

        if os.path.exists(output):
            print("Output already exists: " + output)
        else:
            options, var_objs, att_objs = surfex.forcing.set_forcing_config(**kwargs)
            surfex.forcing.run_time_loop(options, var_objs, att_objs)
