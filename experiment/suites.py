import sys
import os
from datetime import datetime, timedelta
import scheduler


class SurfexSuite(scheduler.SuiteDefinition):

    def __init__(self, suite_name, exp, joboutdir, env_submit, server_config, server_log, dtgs, def_file, dtgbeg=None):

        if dtgbeg is None:
            dtgbeg_str = dtgs[0].strftime("%Y%m%d%H")
        else:
            dtgbeg_str = dtgbeg.strftime("%Y%m%d%H")

        # Scheduler settings
        # ecf_loghost = exp.server.get_var("ECF_LOGHOST")
        # ecf_logport = exp.server.get_var("ECF_LOGPORT")

        # TODO use SFX_DATA
        lib = exp.wd + ""

        ecf_include = lib + "/ecf"
        ecf_files = lib + "/ecf"
        ecf_home = joboutdir
        ecf_out = joboutdir
        ecf_jobout = joboutdir + "/%ECF_NAME%.%ECF_TRYNO%"
        # server_log = exp.get_file_name(lib, "server_log", full_path=True)

        pythonpath = "export PYTHONPATH="
        pythonpath = pythonpath + "%LIB%/pysurfex/:"
        pythonpath = pythonpath + "" + exp.wd + "/pysurfex/:"
        pythonpath = pythonpath + "" + exp.conf
        if "PYTHONPATH" in os.environ:
            pythonpath = pythonpath + ":" + os.path.expandvars(os.environ["PYTHONPATH"])
        pythonpath = pythonpath + ";"

        path = "export PATH="
        path = path + "%LIB%/pysurfex/bin:"
        path = path + "" + exp.wd + "/pysurfex/bin:"
        path = path + "" + exp.conf + "/bin"
        path = path + ":$PATH;"

        ecf_job_cmd = pythonpath + " " + path + " " + \
                                   "ECF_submit_exp " \
                                   "-ensmbr %ENSMBR% " \
                                   "-dtg %DTG% " + \
                                   "-exp %EXP% " \
                                   "-lib %LIB% " \
                                   "-ecf_name %ECF_NAME% " \
                                   "-ecf_tryno %ECF_TRYNO% " \
                                   "-ecf_pass %ECF_PASS% " \
                                   "-ecf_rid %ECF_RID%"
        ecf_kill_cmd = pythonpath + " " + path + " " + \
                                    "ECF_kill_exp " \
                                    "-exp %EXP% " \
                                    "-lib %LIB% " \
                                    "-ecf_name %ECF_NAME%" \
                                    "-ecf_tryno %ECF_TRYNO% " \
                                    "-ecf_pass %ECF_PASS% " \
                                    "-ecf_rid %ECF_RID% " \
                                    "-submission_id %SUBMISSION_ID%"
        ecf_status_cmd = pythonpath + " " + path + " " + \
                                      "ECF_status_exp " \
                                      "-exp %EXP% " \
                                      "-lib %LIB% " \
                                      "-ecf_name %ECF_NAME% " \
                                      "-ecf_tryno %ECF_TRYNO% "\
                                      "-ecf_pass %ECF_PASS% " \
                                      "-ecf_rid %ECF_RID% " \
                                      "-submission_id %SUBMISSION_ID%"

        scheduler.SuiteDefinition.__init__(self, suite_name, def_file, joboutdir, ecf_files, env_submit,
                                           server_config, server_log,
                                           ecf_home=ecf_home, ecf_include=ecf_include, ecf_out=ecf_out,
                                           ecf_jobout=ecf_jobout,
                                           ecf_job_cmd=ecf_job_cmd,
                                           ecf_status_cmd=ecf_status_cmd,
                                           ecf_kill_cmd=ecf_kill_cmd,
                                           pythonpath=pythonpath, path=path)

        self.suite.ecf_node.add_variable("LIB", lib)
        self.suite.ecf_node.add_variable("EXP", exp.name)
        self.suite.ecf_node.add_variable("DTG", dtgbeg_str)
        self.suite.ecf_node.add_variable("DTGBEG", dtgbeg_str)
        # self.suite.ecf_node.add_variable("STREAM", "")
        # self.suite.ecf_node.add_variable("ENSMBR", "")
        self.suite.ecf_node.add_variable("ARGS", "")

        # self.suite = EcflowSuite(self.suite_name, def_file=def_file, variables=variables)

        init_run = scheduler.EcflowSuiteTask("InitRun", self.suite)
        init_run_complete = scheduler.EcflowSuiteTrigger(init_run)

        if exp.config.get_setting("COMPILE#BUILD"):
            comp_trigger = scheduler.EcflowSuiteTriggers(init_run_complete)
            comp = scheduler.EcflowSuiteFamily("Compilation", self.suite, triggers=comp_trigger)

            scheduler.EcflowSuiteTask("MakeOfflineBinaries", comp, ecf_files=ecf_files)

            comp_complete = scheduler.EcflowSuiteTrigger(comp, mode="complete")
        else:
            comp_complete = None

        static = scheduler.EcflowSuiteFamily("StaticData", self.suite,
                                             triggers=scheduler.EcflowSuiteTriggers([init_run_complete, comp_complete]))
        scheduler.EcflowSuiteTask("Pgd", static, ecf_files=ecf_files)

        static_complete = scheduler.EcflowSuiteTrigger(static)

        prep_complete = None
        hours_ahead = 24
        cycle_input_dtg_node = {}
        prediction_dtg_node = {}
        post_processing_dtg_node = {}
        prev_dtg = None
        for dtg in dtgs:
            dtg_str = dtg.strftime("%Y%m%d%H")
            variables = [
                scheduler.EcflowSuiteVariable("DTG", dtg_str),
                scheduler.EcflowSuiteVariable("DTGBEG", dtgbeg_str)
            ]
            triggers = scheduler.EcflowSuiteTriggers([init_run_complete, static_complete])

            dtg_node = scheduler.EcflowSuiteFamily(dtg_str, self.suite, variables=variables, triggers=triggers)

            ahead_trigger = None
            for dtg_str2 in prediction_dtg_node:
                validtime = datetime.strptime(dtg_str2, "%Y%m%d%H")
                if validtime < dtg:
                    if validtime + timedelta(hours=hours_ahead) <= dtg:
                        ahead_trigger = scheduler.EcflowSuiteTrigger(prediction_dtg_node[dtg_str2])

            if ahead_trigger is None:
                triggers = scheduler.EcflowSuiteTriggers([init_run_complete, static_complete])
            else:
                triggers = scheduler.EcflowSuiteTriggers([init_run_complete, static_complete, ahead_trigger])

            prepare_cycle = scheduler.EcflowSuiteTask("PrepareCycle", dtg_node, triggers=triggers, ecf_files=ecf_files)
            prepare_cycle_complete = scheduler.EcflowSuiteTrigger(prepare_cycle)

            triggers.add_triggers(scheduler.EcflowSuiteTrigger(prepare_cycle))

            cycle_input = scheduler.EcflowSuiteFamily("CycleInput", dtg_node, triggers=triggers)
            cycle_input_dtg_node.update({dtg_str: cycle_input})

            scheduler.EcflowSuiteTask("Forcing", cycle_input, ecf_files=ecf_files)

            triggers = scheduler.EcflowSuiteTriggers([init_run_complete, static_complete, prepare_cycle_complete])
            if prev_dtg is not None:
                prev_dtg_str = prev_dtg.strftime("%Y%m%d%H")
                triggers.add_triggers(scheduler.EcflowSuiteTrigger(prediction_dtg_node[prev_dtg_str]))

            ############################################################################################################
            initialization = scheduler.EcflowSuiteFamily("Initialization", dtg_node, triggers=triggers)

            analysis = None
            if dtg == dtgbeg:

                prep = scheduler.EcflowSuiteTask("Prep", initialization, ecf_files=ecf_files)
                prep_complete = scheduler.EcflowSuiteTrigger(prep)
                # Might need an extra trigger for input

            else:
                schemes = exp.config.get_setting("SURFEX#ASSIM#SCHEMES")
                do_soda = False
                for scheme in schemes:
                    if schemes[scheme].upper() != "NONE":
                        do_soda = True

                do_snow_ass = False
                nnco = exp.config.get_setting("SURFEX#ASSIM#OBS#NNCO")
                for ivar in range(0, len(nnco)):
                    if nnco[ivar] == 0:
                        if ivar == 0:
                            pass
                        elif ivar == 1:
                            pass
                        elif ivar == 4:
                            do_snow_ass = True

                if do_snow_ass:
                    do_snow_ass = False
                    snow_ass = exp.config.get_setting("SURFEX#ASSIM#ISBA#UPDATE_SNOW_CYCLES")
                    if len(snow_ass) > 0:
                        hh = int(dtg.strftime("%H"))
                        for sn in snow_ass:
                            if hh == int(sn):
                                print("Do snow assimilation for ", dtg)
                                do_soda = True
                                do_snow_ass = True

                triggers = scheduler.EcflowSuiteTriggers(prep_complete)
                if not do_soda:
                    scheduler.EcflowSuiteTask("CycleFirstGuess", initialization, triggers=triggers, ecf_files=ecf_files)
                else:
                    fg = scheduler.EcflowSuiteTask("FirstGuess", initialization, triggers=triggers, ecf_files=ecf_files)

                    perturbations = None
                    if exp.config.setting_is("SURFEX#ASSIM#SCHEMES#ISBA", "EKF"):

                        perturbations = scheduler.EcflowSuiteFamily("Perturbations", initialization)
                        nncv = exp.config.get_setting("SURFEX#ASSIM#ISBA#EKF#NNCV")
                        names = exp.config.get_setting("SURFEX#ASSIM#ISBA#EKF#CVAR_M")
                        triggers = None
                        hh = exp.progress.dtg.strftime("%H")
                        mbr = None
                        fcint = exp.config.get_fcint(hh, mbr=mbr)
                        fg_dtg = (exp.progress.dtg - timedelta(hours=fcint)).strftime("%Y%m%d%H")
                        if fg_dtg in cycle_input_dtg_node:
                            triggers = scheduler.EcflowSuiteTriggers(
                                scheduler.EcflowSuiteTrigger(cycle_input_dtg_node[fg_dtg]))
                        for ivar in range(0, len(nncv)):
                            print(nncv[ivar])
                            if ivar == 0:
                                name = "REF"
                                args = "pert=" + str(ivar) + " name=" + name
                                print(args)
                                variables = scheduler.EcflowSuiteVariable("ARGS", args)

                                pert = scheduler.EcflowSuiteFamily(name, perturbations, variables=variables)
                                scheduler.EcflowSuiteTask("PerturbedRun", pert, ecf_files=ecf_files,
                                                          triggers=triggers)
                            if nncv[ivar] == 1:
                                name = names[ivar]
                                args = "pert=" + str(ivar + 1) + " name=" + name
                                print(args)
                                variables = scheduler.EcflowSuiteVariable("ARGS", args)
                                pert = scheduler.EcflowSuiteFamily(name, perturbations, variables=variables)
                                scheduler.EcflowSuiteTask("PerturbedRun", pert, ecf_files=ecf_files,
                                                          triggers=triggers)

                    prepare_oi_soil_input = None
                    prepare_oi_climate = None
                    if exp.config.setting_is("SURFEX#ASSIM#SCHEMES#ISBA", "OI"):
                        prepare_oi_soil_input = scheduler.EcflowSuiteTask("PrepareOiSoilInput", initialization,
                                                                          ecf_files=ecf_files)
                        prepare_oi_climate = scheduler.EcflowSuiteTask("PrepareOiClimate", initialization,
                                                                       ecf_files=ecf_files)

                    prepare_sst = None
                    if exp.config.setting_is("SURFEX#ASSIM#SCHEMES#SEA", "INPUT"):
                        if exp.config.setting_is("SURFEX#ASSIM#SEA#CFILE_FORMAT_SST", "ASCII"):
                            prepare_sst = scheduler.EcflowSuiteTask("PrepareSST", initialization, ecf_files=ecf_files)

                    an_variables = {"t2m": False, "rh2m": False, "sd": False}
                    nnco = exp.config.get_setting("SURFEX#ASSIM#OBS#NNCO")
                    for ivar in range(0, len(nnco)):
                        if nnco[ivar] == 1:
                            if ivar == 0:
                                an_variables.update({"t2m": True})
                            elif ivar == 1:
                                an_variables.update({"rh2m": True})
                            elif ivar == 4:
                                if do_snow_ass:
                                    an_variables.update({"sd": True})

                    analysis = scheduler.EcflowSuiteFamily("Analysis", initialization)
                    fg4oi = scheduler.EcflowSuiteTask("FirstGuess4OI", analysis, ecf_files=ecf_files)
                    fg4oi_complete = scheduler.EcflowSuiteTrigger(fg4oi)

                    triggers = []
                    for var in an_variables:
                        if an_variables[var]:
                            v = scheduler.EcflowSuiteFamily(var, analysis)
                            qc_triggers = None
                            if var == "sd":
                                qc_triggers = scheduler.EcflowSuiteTriggers(fg4oi_complete)
                            qc = scheduler.EcflowSuiteTask("QualityControl", v, triggers=qc_triggers,
                                                           ecf_files=ecf_files)
                            oi_triggers = scheduler.EcflowSuiteTriggers([
                                scheduler.EcflowSuiteTrigger(qc), scheduler.EcflowSuiteTrigger(fg4oi)])
                            scheduler.EcflowSuiteTask("OptimalInterpolation", v, triggers=oi_triggers,
                                                      ecf_files=ecf_files)
                            triggers.append(scheduler.EcflowSuiteTrigger(v))

                    oi2soda_complete = None
                    if len(triggers) > 0:
                        triggers = scheduler.EcflowSuiteTriggers(triggers)
                        oi2soda = scheduler.EcflowSuiteTask("Oi2soda", analysis, triggers=triggers, ecf_files=ecf_files)
                        oi2soda_complete = scheduler.EcflowSuiteTrigger(oi2soda)

                    prepare_lsm = None
                    need_lsm = False
                    if exp.config.setting_is("SURFEX#ASSIM#SCHEMES#ISBA", "OI"):
                        need_lsm = True
                    if exp.config.setting_is("SURFEX#ASSIM#SCHEMES#INLAND_WATER", "WATFLX"):
                        if exp.config.get_setting("SURFEX#ASSIM#INLAND_WATER#LEXTRAP_WATER"):
                            need_lsm = True
                    if need_lsm:
                        triggers = scheduler.EcflowSuiteTriggers(fg4oi_complete)
                        prepare_lsm = scheduler.EcflowSuiteTask("PrepareLSM", initialization, ecf_files=ecf_files,
                                                                triggers=triggers)

                    triggers = [scheduler.EcflowSuiteTrigger(fg), oi2soda_complete]
                    if perturbations is not None:
                        triggers.append(scheduler.EcflowSuiteTrigger(perturbations))
                    if prepare_oi_soil_input is not None:
                        triggers.append(scheduler.EcflowSuiteTrigger(prepare_oi_soil_input))
                    if prepare_oi_climate is not None:
                        triggers.append(scheduler.EcflowSuiteTrigger(prepare_oi_climate))
                    if prepare_sst is not None:
                        triggers.append(scheduler.EcflowSuiteTrigger(prepare_sst))
                    if prepare_lsm is not None:
                        triggers.append(scheduler.EcflowSuiteTrigger(prepare_lsm))

                    triggers = scheduler.EcflowSuiteTriggers(triggers)
                    scheduler.EcflowSuiteTask("Soda", analysis, triggers=triggers, ecf_files=ecf_files)

            triggers = scheduler.EcflowSuiteTriggers([scheduler.EcflowSuiteTrigger(cycle_input),
                                                      scheduler.EcflowSuiteTrigger(initialization)])
            prediction = scheduler.EcflowSuiteFamily("Prediction", dtg_node, triggers=triggers)
            prediction_dtg_node.update({dtg_str: prediction})

            forecast = scheduler.EcflowSuiteTask("Forecast", prediction, ecf_files=ecf_files)
            scheduler.EcflowSuiteTask("LogProgress", prediction,
                                      triggers=scheduler.EcflowSuiteTriggers(scheduler.EcflowSuiteTrigger(forecast)),
                                      ecf_files=ecf_files)

            pp = scheduler.EcflowSuiteFamily("PostProcessing", dtg_node,
                                             triggers=scheduler.EcflowSuiteTriggers(
                                                 scheduler.EcflowSuiteTrigger(prediction)))
            post_processing_dtg_node.update({dtg_str: pp})

            log_pp_trigger = None
            if analysis is not None:
                qc2obsmon = scheduler.EcflowSuiteTask("Qc2obsmon", pp, ecf_files=ecf_files)
                log_pp_trigger = scheduler.EcflowSuiteTriggers(scheduler.EcflowSuiteTrigger(qc2obsmon))

            scheduler.EcflowSuiteTask("LogProgressPP", pp, triggers=log_pp_trigger, ecf_files=ecf_files)
            prev_dtg = dtg

        hours_behind = 24
        for dtg in dtgs:
            dtg_str = dtg.strftime("%Y%m%d%H")
            pp_dtg_str = (dtg - timedelta(hours=hours_behind)).strftime("%Y%m%d%H")
            if pp_dtg_str in post_processing_dtg_node:
                triggers = scheduler.EcflowSuiteTriggers(
                    scheduler.EcflowSuiteTrigger(post_processing_dtg_node[pp_dtg_str]))
                cycle_input_dtg_node[dtg_str].add_part_trigger(triggers)

    def save_as_defs(self):
        self.suite.save_as_defs()


class UnitTestSuite(scheduler.SuiteDefinition):
    def __init__(self, suite_name, exp, def_file, joboutdir, ecf_files, env_submit, server_config, server_log):

        # TODO use SFX_DATA
        lib = exp.wd + ""

        ecf_include = lib + "/ecf"
        ecf_files = lib + "/ecf"
        ecf_home = joboutdir
        ecf_out = joboutdir
        ecf_jobout = joboutdir + "/%ECF_NAME%.%ECF_TRYNO%"
        # server_log = exp.get_file_name(lib, "server_log", full_path=True)

        pythonpath = "export PYTHONPATH="
        pythonpath = pythonpath + "%LIB%/pysurfex/:"
        pythonpath = pythonpath + "" + exp.wd + "/pysurfex/:"
        pythonpath = pythonpath + "" + exp.conf
        if "PYTHONPATH" in os.environ:
            pythonpath = pythonpath + ":" + os.path.expandvars(os.environ["PYTHONPATH"])
        pythonpath = pythonpath + ";"

        path = "export PATH="
        path = path + "%LIB%/pysurfex/bin:"
        path = path + "" + exp.wd + "/pysurfex/bin:"
        path = path + "" + exp.conf + "/bin"
        path = path + ":$PATH;"

        ecf_job_cmd = pythonpath + " " + path + " " + \
                                   "ECF_submit_exp " \
                                   "-ensmbr %ENSMBR% " \
                                   "-dtg %DTG% " + \
                                   "-exp %EXP% " \
                                   "-lib %LIB% " \
                                   "-ecf_name %ECF_NAME% " \
                                   "-ecf_tryno %ECF_TRYNO% " \
                                   "-ecf_pass %ECF_PASS% " \
                                   "-ecf_rid %ECF_RID%"
        ecf_kill_cmd = pythonpath + " " + path + " " + \
                                    "ECF_kill_exp " \
                                    "-exp %EXP% " \
                                    "-lib %LIB% " \
                                    "-ecf_name %ECF_NAME%" \
                                    "-ecf_tryno %ECF_TRYNO% " \
                                    "-ecf_pass %ECF_PASS% " \
                                    "-ecf_rid %ECF_RID% " \
                                    "-submission_id %SUBMISSION_ID%"
        ecf_status_cmd = pythonpath + " " + path + " " + \
                                      "ECF_status_exp " \
                                      "-exp %EXP% " \
                                      "-lib %LIB% " \
                                      "-ecf_name %ECF_NAME% " \
                                      "-ecf_tryno %ECF_TRYNO% "\
                                      "-ecf_pass %ECF_PASS% " \
                                      "-ecf_rid %ECF_RID% " \
                                      "-submission_id %SUBMISSION_ID%"

        scheduler.SuiteDefinition.__init__(self, suite_name, def_file, joboutdir, ecf_files, env_submit,
                                           server_config, server_log,
                                           ecf_home=ecf_home, ecf_include=ecf_include, ecf_out=ecf_out,
                                           ecf_jobout=ecf_jobout,
                                           ecf_job_cmd=ecf_job_cmd,
                                           ecf_status_cmd=ecf_status_cmd,
                                           ecf_kill_cmd=ecf_kill_cmd,
                                           pythonpath=pythonpath, path=path)

        self.suite.ecf_node.add_variable("LIB", lib)
        self.suite.ecf_node.add_variable("EXP", exp.name)
        self.suite.ecf_node.add_variable("DTG", "2020010106")
        self.suite.ecf_node.add_variable("DTGBEG", "2020010100")
        # self.suite.ecf_node.add_variable("STREAM", "")
        # self.suite.ecf_node.add_variable("ENSMBR", "")
        self.suite.ecf_node.add_variable("ARGS", "")

        init_run = scheduler.EcflowSuiteTask("InitRun", self.suite, ecf_files=self.ecf_files)
        triggers = scheduler.EcflowSuiteTriggers(scheduler.EcflowSuiteTrigger(init_run))
        unit_test = scheduler.EcflowSuiteTask("UnitTest", self.suite, ecf_files=self.ecf_files, triggers=triggers)

        triggers = scheduler.EcflowSuiteTriggers(scheduler.EcflowSuiteTrigger(unit_test))
        job_to_manipulate = scheduler.EcflowSuiteTask("SleepingBeauty", self.suite, ecf_files=self.ecf_files,
                                                      triggers=triggers)
        # job_to_manipulate.ecf_node.add_defstatus(ecflow.Defstatus("suspended"))

        triggers = scheduler.EcflowSuiteTriggers(scheduler.EcflowSuiteTrigger(job_to_manipulate, "aborted"))
        scheduler.EcflowSuiteTask("SleepingBeauty2", self.suite, ecf_files=self.ecf_files, triggers=triggers)
        scheduler.EcflowSuiteTask("WakeUpCall", self.suite, ecf_files=self.ecf_files, triggers=triggers,
                                  def_status="suspended")


def get_defs(exp, suite_type, def_file):
    suite_name = exp.name
    joboutdir = exp.system.get_var("JOBOUTDIR", "0")
    env_submit = exp.get_file_name(exp.wd, "submit", full_path=True)
    server_config = exp.get_file_name(exp.wd, "server", full_path=True)
    server_log = exp.server.logfile
    lib = exp.wd + ""
    ecf_files = lib + "/ecf"
    hh_list = exp.config.get_total_unique_hh_list()
    dtgstart = exp.progress.dtg
    dtgbeg = exp.progress.dtgbeg
    dtgend = exp.progress.dtgend
    print(dtgstart, dtgbeg, dtgend)
    if dtgbeg is None:
        dtgbeg = dtgstart
    dtgs = []
    dtg = dtgstart
    while dtg <= dtgend:
        dtgs.append(dtg)
        hh = dtg.strftime("%H")
        fcint = None
        if len(hh_list) > 1:
            for h in range(0, len(hh_list)):
                print(h, hh_list[h], hh)
                if int(hh_list[h]) == int(hh):
                    if h == len(hh_list) - 1:
                        fcint = ((int(hh_list[len(hh_list) - 1]) % 24) - int(hh_list[0])) % 24
                    else:
                        fcint = int(hh_list[h + 1]) - int(hh_list[h])
        else:
            fcint = 24
        if fcint is None:
            raise Exception
        dtg = dtg + timedelta(hours=fcint)
    if suite_type == "surfex":
        return SurfexSuite(suite_name, exp, joboutdir, env_submit, server_config, server_log, dtgs, def_file,
                           dtgbeg=dtgbeg)
        # return None
    elif suite_type == "unittest":
        defs = UnitTestSuite(suite_name, exp, def_file, joboutdir, ecf_files, env_submit, server_config, server_log)
        return defs
    else:
        raise Exception()
