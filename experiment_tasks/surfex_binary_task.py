from experiment_tasks import AbstractTask
import surfex
import os
from datetime import timedelta


class SurfexBinaryTask(AbstractTask):
    def __init__(self, task, system, config, exp_file_paths, progress, mode,
                 mbr=None, stream=None, debug=False, **kwargs):
        AbstractTask.__init__(self, task, system, config, exp_file_paths, progress, debug=debug, mbr=mbr,
                              stream=stream, **kwargs)

        self.mode = mode
        self.need_pgd = True
        self.need_prep = True
        self.pgd = False
        self.prep = False
        self.perturbed = False
        self.soda = False
        self.namelist = None

        print(kwargs)
        print_namelist = True
        if "print_namelist" in kwargs:
            print_namelist = kwargs["print_namelist"]
        self.print_namelist = print_namelist
        check_existence = True
        if "check_existence" in kwargs:
            check_existence = kwargs["check_existence"]
        self.check_existence = check_existence
        force = False
        if "force" in kwargs:
            force = kwargs["force"]
        self.force = force
        pert = None
        if "pert" in kwargs:
            pert = int(kwargs["pert"])
        self.pert = pert
        ivar = None
        if "ivar" in kwargs:
            ivar = int(kwargs["ivar"])
        self.ivar = ivar

        xyz = ".exe"
        libdir = self.sfx_exp_vars["SFX_EXP_LIB"]
        xyz_file = libdir + "/xyz"
        if os.path.exists(xyz_file):
            xyz = open(xyz_file, "r").read().rstrip()
        else:
            print(xyz_file + " not found. Assume XYZ=" + xyz)
        self.xyz = xyz

    def execute(self):
        raise NotImplementedError

    def execute_binary(self, binary, output, pgd_file_path=None, prep_file_path=None,
                       archive_data=None, forc_zs=False,
                       masterodb=True, perturbed_file_pattern=None, fcint=3, prep_file=None, prep_filetype=None,
                       prep_pgdfile=None, prep_pgdfiletype=None):

        rte = os.environ
        if self.mode == "pgd":
            self.pgd = True
            self.need_pgd = False
            self.need_prep = False
            input_data = surfex.PgdInputData(self.config, self.exp_file_paths, check_existence=self.check_existence)
        elif self.mode == "prep":
            self.prep = True
            self.need_prep = False
            input_data = surfex.PrepInputData(self.config, self.exp_file_paths, check_existence=self.check_existence,
                                              prep_file=prep_file, prep_pgdfile=prep_pgdfile)
        elif self.mode == "offline":
            input_data = surfex.OfflineInputData(self.config, self.exp_file_paths, check_existence=self.check_existence)
        elif self.mode == "soda":
            self.soda = True
            # print(kwargs)
            input_data = surfex.SodaInputData(self.config, self.exp_file_paths, check_existence=self.check_existence,
                                              dtg=self.dtg, masterodb=masterodb,
                                              perturbed_file_pattern=perturbed_file_pattern)
        elif self.mode == "perturbed":
            self.perturbed = True
            input_data = surfex.OfflineInputData(self.config, self.exp_file_paths, check_existence=self.check_existence)
        else:
            raise NotImplementedError(self.mode + " is not implemented!")

        print("pgd", pgd_file_path)
        print(self.perturbed, self.pert)

        self.namelist = surfex.BaseNamelist(self.mode, self.config, self.input_path, forc_zs=forc_zs,
                                            prep_file=prep_file, prep_filetype=prep_filetype,
                                            prep_pgdfile=prep_pgdfile, prep_pgdfiletype=prep_pgdfiletype,
                                            dtg=self.dtg, fcint=fcint)

        print("rte", rte)
        batch = surfex.BatchJob(rte, wrapper=self.wrapper)

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
            if self.pert > 0:
                print(self.ivar)
                surfex.PerturbedOffline(binary, batch, prepfile, self.ivar, settings, input_data,
                                        pgdfile=pgdfile, surfout=surffile,
                                        archive_data=archive_data,
                                        print_namelist=self.print_namelist)
            else:
                surfex.SURFEXBinary(binary, batch, prepfile, settings, input_data,
                                    pgdfile=pgdfile, surfout=surffile,
                                    archive_data=archive_data,
                                    print_namelist=self.print_namelist)
        elif self.pgd:
            pgdfile = surfex.file.PGDFile(filetype, pgdfile, self.geo, input_file=pgd_file_path,
                                          archive_file=output, lfagmap=lfagmap)
            # print(input_data.data)
            surfex.SURFEXBinary(binary, batch, pgdfile, settings, input_data,
                                archive_data=archive_data, print_namelist=self.print_namelist)
        elif self.prep:
            prepfile = surfex.PREPFile(filetype, prepfile, self.geo, archive_file=output,
                                       lfagmap=lfagmap)
            surfex.SURFEXBinary(binary, batch, prepfile, settings, input_data, pgdfile=pgdfile,
                                archive_data=archive_data, print_namelist=self.print_namelist)
        else:
            surfex.SURFEXBinary(binary, batch, prepfile, settings, input_data, pgdfile=pgdfile,
                                surfout=surffile, archive_data=archive_data,
                                print_namelist=self.print_namelist)


class Pgd(SurfexBinaryTask):
    def __init__(self, task, config, system, exp_file_paths, progress, debug=False, mbr=None,
                 stream=None, **kwargs):
        SurfexBinaryTask.__init__(self, task, config, system, exp_file_paths, progress, "pgd", debug=debug, mbr=mbr,
                                  stream=stream, **kwargs)

    def execute(self):
        pgdfile = self.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        output = self.exp_file_paths.get_system_file("pgd_dir", pgdfile, default_dir="default_climdir")
        binary = self.bindir + "/PGD" + self.xyz

        if not os.path.exists(output) or self.force:
            SurfexBinaryTask.execute_binary(self, binary=binary, output=output)
        else:
            print("Output already exists: ", output)


class Prep(SurfexBinaryTask):
    def __init__(self, task, config, system, exp_file_paths, progress, mbr=None, stream=None, debug=False, **kwargs):
        SurfexBinaryTask.__init__(self, task, config, system, exp_file_paths, progress, "prep", debug=debug, mbr=mbr,
                                  stream=stream, **kwargs)

    def execute(self):

        pgdfile = self.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        pgd_file_path = self.exp_file_paths.get_system_file("pgd_dir", pgdfile, default_dir="default_climdir")
        prep_file = self.get_setting("INITIAL_CONDITIONS#PREP_INPUT_FILE",
                                     validtime=self.dtg, basedtg=self.fg_dtg)
        prep_filetype = self.get_setting("INITIAL_CONDITIONS#PREP_INPUT_FILETYPE")
        prep_pgdfile = self.get_setting("INITIAL_CONDITIONS#PREP_PGDFILE")
        prep_pgdfiletype = self.get_setting("INITIAL_CONDITIONS#PREP_PGDFILETYPE")
        prepfile = self.get_setting("SURFEX#IO#CPREPFILE") + self.suffix
        output = self.exp_file_paths.get_system_file("prep_dir", prepfile, basedtg=self.dtg,
                                                     default_dir="default_archive_dir")
        binary = self.bindir + "/PREP" + self.xyz

        if not os.path.exists(output) or self.force:

            SurfexBinaryTask.execute_binary(self, binary, output, pgd_file_path=pgd_file_path,
                                            prep_file=prep_file,
                                            prep_filetype=prep_filetype, prep_pgdfile=prep_pgdfile,
                                            prep_pgdfiletype=prep_pgdfiletype)
        else:
            print("Output already exists: ", output)

        # PREP should prepare for forecast
        if os.path.exists(self.fc_start_sfx):
            os.unlink(self.fc_start_sfx)
        os.symlink(output, self.fc_start_sfx)


class Forecast(SurfexBinaryTask):
    def __init__(self, task, config, system, exp_file_paths, progress, debug=False, mbr=None,
                 stream=None, **kwargs):
        SurfexBinaryTask.__init__(self, task, config, system, exp_file_paths, progress, "offline", debug=debug, mbr=mbr,
                                  stream=stream, **kwargs)

    def execute(self):

        pgdfile = self.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        pgd_file_path = self.exp_file_paths.get_system_file("pgd_dir", pgdfile, default_dir="default_climdir")
        binary = self.bindir + "/OFFLINE" + self.xyz
        forc_zs = self.get_setting("FORECAST#FORC_ZS")

        output = self.archive + "/" + self.get_setting("SURFEX#IO#CSURFFILE") + self.suffix

        archive_data = None
        if self.get_setting("SURFEX#IO#CTIMESERIES_FILETYPE") == "NC":
            last_ll = self.dtg + timedelta(hours=self.fcint)

            fname = "SURFOUT." + last_ll.strftime("%Y%m%d") + "_" + last_ll.strftime("%H") + "h" + \
                    last_ll.strftime("%M") + ".nc"
            archive_data = surfex.JsonOutputData({fname: self.archive + "/" + fname})
            print(archive_data)

        # Forcing dir
        self.exp_file_paths.add_system_file_path("forcing_dir", self.exp_file_paths.get_system_path(
            "forcing_dir", mbr=self.mbr, basedtg=self.dtg, default_dir="default_forcing_dir"))

        if not os.path.exists(output) or self.force:
            SurfexBinaryTask.execute_binary(self, binary, output,
                                            pgd_file_path=pgd_file_path, prep_file_path=self.fc_start_sfx,
                                            forc_zs=forc_zs, archive_data=archive_data)
        else:
            print("Output already exists: ", output)

    def postfix(self):
        pass


class PerturbedRun(SurfexBinaryTask):
    def __init__(self, task, config, system, exp_file_paths, progress, debug=False, mbr=None,
                 stream=None, **kwargs):
        SurfexBinaryTask.__init__(self, task, config, system, exp_file_paths, progress, "perturbed", debug=debug,
                                  mbr=mbr, stream=stream, **kwargs)

    def execute(self):

        pgdfile = self.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        pgd_file_path = self.exp_file_paths.get_system_file("pgd_dir", pgdfile, default_dir="default_climdir")
        bindir = self.exp_file_paths.get_system_path("bin_dir", default_dir="default_bin_dir")
        binary = bindir + "/OFFLINE" + self.xyz
        forc_zs = self.get_setting("FORECAST#FORC_ZS")

        # PREP file is previous analysis unless first assimilation cycle
        if self.fg_dtg == self.dtgbeg:
            prepfile = self.get_setting("SURFEX#IO#CPREPFILE") + self.suffix
        else:
            prepfile = "ANALYSIS" + self.suffix

        prep_file_path = self.exp_file_paths.get_system_file("prep_dir", prepfile, mbr=self.mbr, basedtg=self.fg_dtg,
                                                             default_dir="default_archive_dir")

        output = self.archive + "/" + self.get_setting("SURFEX#IO#CSURFFILE") + "_PERT" + str(self.pert) + self.suffix

        # Forcing dir is for previous cycle
        # TODO If pertubed runs moved to pp it should be a diffenent dtg
        self.exp_file_paths.add_system_file_path("forcing_dir", self.exp_file_paths.get_system_path(
            "forcing_dir", mbr=self.mbr, basedtg=self.fg_dtg, default_dir="default_forcing_dir"))

        if not os.path.exists(output) or self.force:
            SurfexBinaryTask.execute_binary(self, binary, output,
                                            pgd_file_path=pgd_file_path,
                                            forc_zs=forc_zs,
                                            prep_file_path=prep_file_path)
        else:
            print("Output already exists: ", output)

    # Make sure we don't clean yet
    def postfix(self):
        pass


class Soda(SurfexBinaryTask):
    def __init__(self, task, config, system, exp_file_paths, progress, debug=False, mbr=None,
                 stream=None, **kwargs):
        SurfexBinaryTask.__init__(self, task, config, system, exp_file_paths, progress, "soda", debug=debug, mbr=mbr,
                                  stream=stream, **kwargs)

    def execute(self):
        bindir = self.exp_file_paths.get_system_path("bin_dir", default_dir="default_bin_dir")
        binary = bindir + "/SODA" + self.xyz

        pgdfile = self.get_setting("SURFEX#IO#CPGDFILE") + self.suffix
        pgd_file_path = self.exp_file_paths.get_system_file("pgd_dir", pgdfile, default_dir="default_climdir")

        prep_file_path = self.fg_guess_sfx
        output = self.archive + "/ANALYSIS" + self.suffix
        perturbed_file_pattern = None
        if self.config.setting_is("SURFEX#ASSIM#SCHEMES#ISBA", "EKF"):
            # TODO If pertubed runs moved to pp it should be a diffenent dtg
            perturbed_run_dir = self.exp_file_paths.get_system_path("archive_dir",
                                                                    default_dir="default_archive_dir")
            self.exp_file_paths.add_system_file_path("perturbed_run_dir", perturbed_run_dir,
                                                     mbr=self.mbr, basedtg=self.dtg)
            first_guess_dir = self.exp_file_paths.get_system_path("default_archive_dir", mbr=self.mbr,
                                                                  validtime=self.dtg, basedtg=self.fg_dtg)
            self.exp_file_paths.add_system_file_path("first_guess_dir", first_guess_dir)

            perturbed_file_pattern = self.get_setting("SURFEX#IO#CSURFFILE") + "_PERT@PERT@" + self.suffix

        if not os.path.exists(output) or self.force:
            SurfexBinaryTask.execute_binary(self, binary, output, pgd_file_path=pgd_file_path,
                                            prep_file_path=prep_file_path,
                                            perturbed_file_pattern=perturbed_file_pattern)
        else:
            print("Output already exists: ", output)

        # SODA should prepare for forecast
        if os.path.exists(self.fc_start_sfx):
            os.unlink(self.fc_start_sfx)
        os.symlink(output, self.fc_start_sfx)

    # Make sure we don't clean yet
    def postfix(self):
        pass
