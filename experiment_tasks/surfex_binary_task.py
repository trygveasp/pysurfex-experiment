from experiment_tasks import AbstractTask
import surfex
import os
from datetime import timedelta


class SurfexBinaryTask(AbstractTask):
    def __init__(self, task, system, config, exp_file_paths, progress, mode, **kwargs):
        AbstractTask.__init__(self, task, system, config, exp_file_paths, progress, **kwargs)
        self.mode = mode
        self.need_pgd = True
        self.need_prep = True
        self.pgd = False
        self.prep = False
        self.perturbed = False
        self.soda = False
        self.namelist = None

    def execute(self, binary, output, **kwargs):

        rte = os.environ
        wrapper = ""
        if "wrapper" in kwargs:
            wrapper = kwargs["wrapper"]

        if self.mode == "pgd":
            self.pgd = True
            self.need_pgd = False
            self.need_prep = False
            input_data = surfex.PgdInputData(self.config, self.exp_file_paths, **kwargs)
        elif self.mode == "prep":
            self.prep = True
            self.need_prep = False
            input_data = surfex.PrepInputData(self.config, self.exp_file_paths, **kwargs)
        elif self.mode == "offline":
            input_data = surfex.OfflineInputData(self.config, self.exp_file_paths, **kwargs)
        elif self.mode == "soda":
            self.soda = True
            print(kwargs)
            input_data = surfex.SodaInputData(self.config, self.exp_file_paths, **kwargs)
        elif self.mode == "perturbed":
            self.perturbed = True
            input_data = surfex.OfflineInputData(self.config, self.exp_file_paths, **kwargs)
        else:
            raise NotImplementedError(self.mode + " is not implemented!")

        pgd_file_path = None
        if "pgd_file_path" in kwargs:
            pgd_file_path = kwargs["pgd_file_path"]
        prep_file_path = None
        if "prep_file_path" in kwargs:
            prep_file_path = kwargs["prep_file_path"]

        print_namelist = True
        if "print_namelist" in kwargs:
            print_namelist = kwargs["print_namelist"]

        pert = None
        if "pert" in kwargs:
            pert = kwargs["pert"]

        print("pgd", pgd_file_path)
        print(self.perturbed, pert)

        print("kwargs", kwargs)
        self.namelist = surfex.BaseNamelist(self.mode, self.config, self.input_path, **kwargs)

        print("rte", rte)
        batch = surfex.BatchJob(rte, wrapper=wrapper)

        archive_data = None
        if "archive_data" in kwargs:
            archive_data = kwargs["archive_data"]

        # settings = surfex.ascii2nml(json_settings)
        settings = self.namelist.get_namelist()
        self.geo.update_namelist(settings)

        # Create input
        # my_ecoclimap = surfex.JsonInputDataFromFile(ecoclimap_file)
        filetype = settings["nam_io_offline"]["csurf_filetype"]
        pgdfile = settings["nam_io_offline"]["cpgdfile"]
        prepfile = settings["nam_io_offline"]["cprepfile"]
        surffile = settings["nam_io_offline"]["csurffile"]
        lfagmap = False
        if "LFAGMAP" in settings["NAM_IO_OFFLINE"]:
            lfagmap = settings["NAM_IO_OFFLINE"]["LFAGMAP"]

        if self.need_pgd:
            pgdfile = surfex.file.PGDFile(filetype, pgdfile, self.geo, input_file=pgd_file_path,
                                          lfagmap=lfagmap)

        if self.need_prep:
            prepfile = surfex.PREPFile(filetype, prepfile, self.geo, input_file=prep_file_path,
                                       lfagmap=lfagmap)

        if self.need_prep and self.need_pgd:
            surffile = surfex.SURFFile(filetype, surffile, self.geo, archive_file=output, lfagmap=lfagmap)
        else:
            surffile = None

        if self.perturbed:
            surfex.PerturbedOffline(binary, batch, prepfile, pert, settings, input_data,
                                    pgdfile=pgdfile, surfout=surffile,
                                    archive_data=archive_data,
                                    print_namelist=print_namelist)
        elif self.pgd:
            pgdfile = surfex.file.PGDFile(filetype, pgdfile, self.geo, input_file=pgd_file_path,
                                          archive_file=output, lfagmap=lfagmap)
            # print(input_data.data)
            surfex.SURFEXBinary(binary, batch, pgdfile, settings, input_data,
                                archive_data=archive_data, print_namelist=print_namelist)
        elif self.prep:
            prepfile = surfex.PREPFile(filetype, prepfile, self.geo, archive_file=output,
                                       lfagmap=lfagmap)
            surfex.SURFEXBinary(binary, batch, prepfile, settings, input_data, pgdfile=pgdfile,
                                archive_data=archive_data, print_namelist=print_namelist)
        else:
            surfex.SURFEXBinary(binary, batch, prepfile, settings, input_data, pgdfile=pgdfile,
                                surfout=surffile, archive_data=archive_data,
                                print_namelist=print_namelist)


class Pgd(SurfexBinaryTask):
    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        SurfexBinaryTask.__init__(self, task, config, system, exp_file_paths, progress, "pgd", **kwargs)

    def execute(self, **kwargs):
        pgdfile = self.config.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        output = self.exp_file_paths.get_system_file("pgd_dir", pgdfile, default_dir="default_climdir", verbosity=10)
        xyz = self.config.get_setting("COMPILE#XYZ")
        binary = self.bindir + "/PGD" + xyz

        force = False
        if "force" in kwargs:
            force = kwargs["force"]

        if not os.path.exists(output) or force:
            SurfexBinaryTask.execute(self, binary, output, **kwargs)
        else:
            print("Output already exists: ", output)


class Prep(SurfexBinaryTask):
    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        SurfexBinaryTask.__init__(self, task, config, system, exp_file_paths, progress, "prep", **kwargs)

    def execute(self, **kwargs):

        print("prep ", kwargs)
        pgdfile = self.config.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        pgd_file_path = self.exp_file_paths.get_system_file("pgd_dir", pgdfile, default_dir="default_climdir")
        prep_file = self.config.get_setting("INITIAL_CONDITIONS#PREP_INPUT_FILE", check_parsing=False, **kwargs)
        prep_file = surfex.SystemFilePaths.substitute_string(prep_file, **kwargs)
        prep_filetype = self.config.get_setting("INITIAL_CONDITIONS#PREP_INPUT_FILETYPE")
        prep_pgdfile = self.config.get_setting("INITIAL_CONDITIONS#PREP_PGDFILE")
        prep_pgdfiletype = self.config.get_setting("INITIAL_CONDITIONS#PREP_PGDFILETYPE")
        prepfile = self.config.get_setting("SURFEX#IO#CPREPFILE") + self.suffix
        output = self.exp_file_paths.get_system_file("prep_dir", prepfile, basedtg=self.dtg,
                                                     default_dir="default_archive_dir")
        xyz = self.config.get_setting("COMPILE#XYZ")
        binary = self.bindir + "/PREP" + xyz

        force = False
        if "force" in kwargs:
            force = kwargs["force"]

        if not os.path.exists(output) or force:

            kwargs = {
                "pgd_file_path": pgd_file_path,
                "prep_file": prep_file,
                "prep_filetype": str(prep_filetype),
                "prep_pgdfile": prep_pgdfile,
                "prep_pgdfiletype": str(prep_pgdfiletype),
                "dtg": self.dtg
            }

            print(kwargs)
            SurfexBinaryTask.execute(self, binary, output, **kwargs)
        else:
            print("Output already exists: ", output)

        # PREP should prepare for forecast
        if os.path.exists(self.fc_start_sfx):
            os.unlink(self.fc_start_sfx)
        os.symlink(output, self.fc_start_sfx)


class Forecast(SurfexBinaryTask):
    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        SurfexBinaryTask.__init__(self, task, config, system, exp_file_paths, progress, "offline", **kwargs)

    def execute(self, **kwargs):

        pgdfile = self.config.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        pgd_file_path = self.exp_file_paths.get_system_file("pgd_dir", pgdfile, default_dir="default_climdir")
        xyz = self.config.get_setting("COMPILE#XYZ")
        binary = self.bindir + "/OFFLINE" + xyz
        forc_zs = self.config.get_setting("FORECAST#FORC_ZS")

        kwargs.update({"pgd_file_path": pgd_file_path})
        kwargs.update({"prep_file_path": self.fc_start_sfx})
        kwargs.update({"forc_zs": forc_zs})
        output = self.archive + "/" + self.config.get_setting("SURFEX#IO#CSURFFILE") + self.suffix

        if self.config.get_setting("SURFEX#IO#CTIMESERIES_FILETYPE") == "NC":
            last_ll = self.dtg + timedelta(hours=self.fcint)

            fname = "SURFOUT." + last_ll.strftime("%Y%m%d") + "_" + last_ll.strftime("%H") + "h" + \
                    last_ll.strftime("%M") + ".nc"
            archive_data = surfex.JsonOutputData({fname: self.archive + "/" + fname})
            print(archive_data)
            kwargs.update({"archive_data": archive_data})

        # Forcing dir
        self.exp_file_paths.add_system_file_path("forcing_dir", self.exp_file_paths.get_system_path(
            "forcing_dir", mbr=self.mbr, basedtg=self.dtg, default_dir="default_forcing_dir"))
        force = False
        if "force" in kwargs:
            force = kwargs["force"]
            del(kwargs["force"])

        if not os.path.exists(output) or force:
            SurfexBinaryTask.execute(self, binary, output, **kwargs)
        else:
            print("Output already exists: ", output)

    def postfix(self, **kwargs):
        pass


class PerturbedRun(SurfexBinaryTask):
    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        SurfexBinaryTask.__init__(self, task, config, system, exp_file_paths, progress, "perturbed", **kwargs)
        self.pert = self.args["pert"]

    def execute(self, **kwargs):
        kwargs.update({"pert": self.pert})

        pgdfile = self.config.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        pgd_file_path = self.exp_file_paths.get_system_file("pgd_dir", pgdfile, default_dir="default_climdir")
        prepfile = self.config.get_setting("SURFEX#IO#CPREPFILE") + self.suffix
        prep_file_path = self.exp_file_paths.get_system_file("prep_dir", prepfile, mbr=self.mbr, basedtg=self.fg_dtg)
        xyz = self.config.get_setting("COMPILE#XYZ")
        bindir = self.exp_file_paths.get_system_path("bin_dir", default_dir="default_bin_dir")
        binary = bindir + "/OFFLINE" + xyz
        forc_zs = self.config.get_setting("FORECAST#FORC_ZS")

        kwargs.update({"pgd_file_path": pgd_file_path})
        kwargs.update({"prep_file_path": prep_file_path})
        kwargs.update({"forc_zs": forc_zs})

        # PREP file is previous analysis unless first assimilation cycle
        if self.fg_dtg == self.dtgbeg:
            prep_file = self.config.get_setting("SURFEX#IO#CPREPFILE") + self.suffix
        else:
            prep_file = "ANALYSIS" + self.suffix

        kwargs.update({"prep_file_path": self.exp_file_paths.get_system_file("prep_dir", prep_file, mbr=self.mbr,
                                                                             basedtg=self.fg_dtg)})

        output = self.archive + "/" + self.config.get_setting("SURFEX#IO#CSURFFILE") + "_PERT" + self.pert + self.suffix

        # Forcing dir is for previous cycle
        # TODO If pertubed runs moved to pp it should be a diffenent dtg
        self.exp_file_paths.add_system_file_path("forcing_dir", self.exp_file_paths.get_system_path(
            "forcing_dir", mbr=self.mbr, basedtg=self.fg_dtg))

        force = False
        if "force" in kwargs:
            force = kwargs["force"]

        if not os.path.exists(output) or force:
            SurfexBinaryTask.execute(self, binary, output, **kwargs)
        else:
            print("Output already exists: ", output)


class Soda(SurfexBinaryTask):
    def __init__(self, task, config, system, exp_file_paths, progress, **kwargs):
        SurfexBinaryTask.__init__(self, task, config, system, exp_file_paths, progress, "soda", **kwargs)

    def execute(self, **kwargs):
        xyz = self.config.get_setting("COMPILE#XYZ")
        bindir = self.exp_file_paths.get_system_path("bin_dir", default_dir="default_bin_dir")
        binary = bindir + "/SODA" + xyz

        pgdfile = self.config.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        pgd_file_path = self.exp_file_paths.get_system_file("pgd_dir", pgdfile, default_dir="default_climdir")

        kwargs.update({"pgd_file_path": pgd_file_path})
        kwargs.update({"prep_file_path": self.fg_guess_sfx})
        kwargs.update({"dtg": self.dtg})
        output = self.archive + "/ANALYSIS" + self.suffix
        if self.config.setting_is("SURFEX#ASSIM#SCHEMES#ISBA", "EKF"):
            # TODO If pertubed runs moved to pp it should be a diffenent dtg
            perturbed_run_dir = self.exp_file_paths.get_system_path("archive_dir", check_parsing=False,
                                                                    default_dir="default_archive_dir")
            self.exp_file_paths.add_system_file_path("perturbed_run_dir", perturbed_run_dir,
                                                     mbr=self.mbr, basedtg=self.dtg)
            perturbed_file_pattern = self.config.get_setting("SURFEX#IO#CSURFFILE") + "_PERT@PERT@" + self.suffix
            kwargs.update({"perturbed_file_pattern": perturbed_file_pattern})

        force = False
        if "force" in kwargs:
            force = kwargs["force"]

        if not os.path.exists(output) or force:
            print(kwargs)
            SurfexBinaryTask.execute(self, binary, output, **kwargs)
        else:
            print("Output already exists: ", output)

        # SODA should prepare for forecast
        if os.path.exists(self.fc_start_sfx):
            os.unlink(self.fc_start_sfx)
        os.symlink(output, self.fc_start_sfx)

    # Make sure we don't clean yet
    def postfix(self, **kwargs):
        pass
