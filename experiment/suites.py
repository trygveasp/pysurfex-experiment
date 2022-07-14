"""Suite for experiment."""
from datetime import datetime, timedelta
import logging
import scheduler


class SurfexSuite(scheduler.SuiteDefinition):
    """Surfex suite."""

    def __init__(self, suite_name, exp, joboutdir, env_submit, dtgs, next_start_dtg, dtgbeg=None):
        """Initialize a SurfexSuite object.

        Args:
            suite_name (str): Name of the suite
            exp (experiment.Experiment): Experiment you want to run
            joboutdir (str): Directory for job and log files
            env_submit (dict): Submission environment for jobs
            dtgs (list): The DTGs you want to run
            next_start_dtg (datetime): Next DTG to run after this DTG
            dtgbeg (datetime, optional): First DTG the experiment run. Defaults to None.

        """
        if dtgbeg is None:
            dtgbeg_str = dtgs[0].strftime("%Y%m%d%H%M")
        else:
            dtgbeg_str = dtgbeg.strftime("%Y%m%d%H%M")

        lib = exp.system.get_var("SFX_EXP_LIB", "0")
        pythonpath = exp.system.get_var("SCHEDULER_PYTHONPATH", "0")
        exp_dir = exp.work_dir + ""
        pythonpath = exp_dir + ":" + pythonpath

        ecf_include = lib + "/ecf"
        ecf_files = lib + "/ecf"
        ecf_home = joboutdir
        ecf_out = joboutdir
        ecf_jobout = joboutdir + "/%ECF_NAME%.%ECF_TRYNO%"
        # server_log = exp.get_file_name(lib, "server_log", full_path=True)

        logging.debug("PYTHONPATH: %s", pythonpath)
        ecf_job_cmd = f"export PYTHONPATH={pythonpath} && " \
                      f"{exp_dir}/bin/ECF_submit_exp " \
                      "-ensmbr %ENSMBR% " \
                      "-dtg %DTG% " + \
                      "-exp " + exp_dir + "/scheduler.json " \
                      "-ecf_name %ECF_NAME% " \
                      "-ecf_tryno %ECF_TRYNO% " \
                      "-ecf_pass %ECF_PASS% " \
                      "-ecf_rid %ECF_RID%"
        ecf_kill_cmd = f"export PYTHONPATH={pythonpath} && " \
                       f"{exp_dir}/bin/ECF_kill_exp " \
                       "-exp " + exp_dir + "/scheduler.json " \
                       "-ecf_name %ECF_NAME% " \
                       "-ecf_tryno %ECF_TRYNO% " \
                       "-ecf_pass %ECF_PASS% " \
                       "-ecf_rid %ECF_RID% " \
                       "-submission_id %SUBMISSION_ID%"
        ecf_status_cmd = f"export PYTHONPATH={pythonpath} && " \
                         f"{exp_dir}/bin/ECF_status_exp " \
                         "-exp " + exp_dir + "/scheduler.json " \
                         "-ecf_name %ECF_NAME% " \
                         "-ecf_tryno %ECF_TRYNO% "\
                         "-ecf_pass %ECF_PASS% " \
                         "-ecf_rid %ECF_RID% " \
                         "-submission_id %SUBMISSION_ID%"

        self.suite_name = suite_name
        scheduler.SuiteDefinition.__init__(self, suite_name, joboutdir, ecf_files, env_submit,
                                           ecf_home=ecf_home, ecf_include=ecf_include,
                                           ecf_out=ecf_out,
                                           ecf_jobout=ecf_jobout,
                                           ecf_job_cmd=ecf_job_cmd,
                                           ecf_status_cmd=ecf_status_cmd,
                                           ecf_kill_cmd=ecf_kill_cmd,
                                           pythonpath="", path="")

        self.suite.ecf_node.add_variable("EXP_DIR", exp_dir)
        self.suite.ecf_node.add_variable("LIB", lib)
        self.suite.ecf_node.add_variable("SERVER_LOGFILE", exp.server.logfile)
        self.suite.ecf_node.add_variable("EXP", exp.name)
        self.suite.ecf_node.add_variable("DTG", dtgbeg_str)
        self.suite.ecf_node.add_variable("DTGBEG", dtgbeg_str)
        # self.suite.ecf_node.add_variable("STREAM", "")
        # self.suite.ecf_node.add_variable("ENSMBR", "")
        self.suite.ecf_node.add_variable("ARGS", "")

        # self.suite = EcflowSuite(self.suite_name, def_file=def_file, variables=variables)

        init_run = scheduler.EcflowSuiteTask("InitRun", self.suite)
        init_run.ecf_node.add_variable("LIB", exp_dir)
        init_run_ecf_job_cmd = f"export PYTHONPATH={pythonpath} && " \
                               f"{exp_dir}/bin/ECF_submit_exp " \
                               "-ensmbr %ENSMBR% " \
                               "-dtg %DTG% " + \
                               "-exp %LIB%/scheduler.json " \
                               "-ecf_name %ECF_NAME% " \
                               "-ecf_tryno %ECF_TRYNO% " \
                               "-ecf_pass %ECF_PASS% " \
                               "-ecf_rid %ECF_RID%"
        init_run_ecf_kill_cmd = f"export PYTHONPATH={pythonpath} && " \
                                f"{exp_dir}/bin/ECF_kill_exp " \
                                "-exp %EXP%/scheduler.json " \
                                "-ecf_name %ECF_NAME% " \
                                "-ecf_tryno %ECF_TRYNO% " \
                                "-ecf_pass %ECF_PASS% " \
                                "-ecf_rid %ECF_RID% " \
                                "-submission_id %SUBMISSION_ID%"
        init_run_ecf_status_cmd = f"export PYTHONPATH={pythonpath} && " \
                                  f"{exp_dir}/bin/ECF_status_exp " \
                                  "-exp %EXP%/scheduler.json " \
                                  "-ecf_name %ECF_NAME% " \
                                  "-ecf_tryno %ECF_TRYNO% "\
                                  "-ecf_pass %ECF_PASS% " \
                                  "-ecf_rid %ECF_RID% " \
                                  "-submission_id %SUBMISSION_ID%"
        init_run.ecf_node.add_variable("ECF_JOB_CMD", init_run_ecf_job_cmd)
        init_run.ecf_node.add_variable("ECF_KILL_CMD", init_run_ecf_kill_cmd)
        init_run.ecf_node.add_variable("ECF_STATUS_CMD", init_run_ecf_status_cmd)
        init_run_complete = scheduler.EcflowSuiteTrigger(init_run)

        if exp.config.get_setting("COMPILE#BUILD"):
            comp_trigger = scheduler.EcflowSuiteTriggers(init_run_complete)
            comp = scheduler.EcflowSuiteFamily("Compilation", self.suite, triggers=comp_trigger)
            configure = scheduler.EcflowSuiteTask("ConfigureOfflineBinaries", comp,
                                                  ecf_files=ecf_files)
            configure_complete = scheduler.EcflowSuiteTrigger(configure, mode="complete")
            scheduler.EcflowSuiteTask("MakeOfflineBinaries", comp, ecf_files=ecf_files,
                                      triggers=scheduler.EcflowSuiteTriggers([configure_complete]))
            comp_complete = scheduler.EcflowSuiteTrigger(comp, mode="complete")
        else:
            comp_complete = None

        triggers = scheduler.EcflowSuiteTriggers([init_run_complete, comp_complete])
        static = scheduler.EcflowSuiteFamily("StaticData", self.suite, triggers=triggers)
        scheduler.EcflowSuiteTask("Pgd", static, ecf_files=ecf_files)

        static_complete = scheduler.EcflowSuiteTrigger(static)

        prep_complete = None
        hours_ahead = 24
        cycle_input_dtg_node = {}
        prediction_dtg_node = {}
        post_processing_dtg_node = {}
        prev_dtg = None
        for idtg, dtg in enumerate(dtgs):
            if idtg < (len(dtgs) - 1):
                next_dtg = dtgs[idtg + 1]
            else:
                next_dtg = next_start_dtg
            next_dtg_str = next_dtg.strftime("%Y%m%d%H%M")
            dtg_str = dtg.strftime("%Y%m%d%H%M")
            variables = [
                scheduler.EcflowSuiteVariable("DTG", dtg_str),
                scheduler.EcflowSuiteVariable("DTG_NEXT", next_dtg_str),
                scheduler.EcflowSuiteVariable("DTGBEG", dtgbeg_str)
            ]
            triggers = scheduler.EcflowSuiteTriggers([init_run_complete, static_complete])

            dtg_node = scheduler.EcflowSuiteFamily(dtg_str, self.suite, variables=variables,
                                                   triggers=triggers)

            ahead_trigger = None
            for dtg_str2, tname in prediction_dtg_node.items():
                validtime = datetime.strptime(dtg_str2, "%Y%m%d%H%M")
                if validtime < dtg:
                    if validtime + timedelta(hours=hours_ahead) <= dtg:
                        ahead_trigger = scheduler.EcflowSuiteTrigger(tname)

            if ahead_trigger is None:
                triggers = scheduler.EcflowSuiteTriggers([init_run_complete, static_complete])
            else:
                triggers = scheduler.EcflowSuiteTriggers([init_run_complete, static_complete,
                                                          ahead_trigger])

            prepare_cycle = scheduler.EcflowSuiteTask("PrepareCycle", dtg_node, triggers=triggers,
                                                      ecf_files=ecf_files)
            prepare_cycle_complete = scheduler.EcflowSuiteTrigger(prepare_cycle)

            triggers.add_triggers(scheduler.EcflowSuiteTrigger(prepare_cycle))

            cycle_input = scheduler.EcflowSuiteFamily("CycleInput", dtg_node, triggers=triggers)
            cycle_input_dtg_node.update({dtg_str: cycle_input})

            scheduler.EcflowSuiteTask("Forcing", cycle_input, ecf_files=ecf_files)

            triggers = scheduler.EcflowSuiteTriggers([init_run_complete, static_complete,
                                                     prepare_cycle_complete])
            if prev_dtg is not None:
                prev_dtg_str = prev_dtg.strftime("%Y%m%d%H%M")
                trigger = scheduler.EcflowSuiteTrigger(prediction_dtg_node[prev_dtg_str])
                triggers.add_triggers(trigger)

            ########################################################################################
            initialization = scheduler.EcflowSuiteFamily("Initialization", dtg_node,
                                                         triggers=triggers)

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
                obs_types = exp.config.get_setting("SURFEX#ASSIM#OBS#COBS_M")
                nnco = exp.config.get_setting("SURFEX#ASSIM#OBS#NNCO")
                for ivar in range(0, len(nnco)):
                    if len(obs_types) > ivar and obs_types[ivar] == "SWE":
                        snow_ass = exp.config.get_setting("SURFEX#ASSIM#ISBA#UPDATE_SNOW_CYCLES")
                        if len(snow_ass) > 0:
                            cycle_hour = int(dtg.strftime("%H"))
                            for sn_cycle in snow_ass:
                                if cycle_hour == int(sn_cycle):
                                    logging.debug("Do snow assimilation for %s", dtg)
                                    do_soda = True
                                    do_snow_ass = True

                triggers = scheduler.EcflowSuiteTriggers(prep_complete)
                if not do_soda:
                    scheduler.EcflowSuiteTask("CycleFirstGuess", initialization, triggers=triggers,
                                              ecf_files=ecf_files)
                else:
                    fg_task = scheduler.EcflowSuiteTask("FirstGuess", initialization,
                                                        triggers=triggers,
                                                        ecf_files=ecf_files)

                    perturbations = None
                    if exp.config.setting_is("SURFEX#ASSIM#SCHEMES#ISBA", "EKF"):

                        perturbations = scheduler.EcflowSuiteFamily("Perturbations", initialization)
                        nncv = exp.config.get_setting("SURFEX#ASSIM#ISBA#EKF#NNCV")
                        names = exp.config.get_setting("SURFEX#ASSIM#ISBA#EKF#CVAR_M")
                        triggers = None
                        mbr = None
                        fgint = exp.config.get_fgint(exp.progress.dtg, mbr=mbr)
                        fg_dtg = (exp.progress.dtg - timedelta(hours=fgint)).strftime("%Y%m%d%H%M")
                        if fg_dtg in cycle_input_dtg_node:
                            triggers = scheduler.EcflowSuiteTriggers(
                                scheduler.EcflowSuiteTrigger(cycle_input_dtg_node[fg_dtg]))

                        nivar = 1
                        for ivar, val in enumerate(nncv):
                            print(ivar, val)
                            logging.debug("ivar %s, nncv[ivar] %s", str(ivar), str(val))
                            if ivar == 0:
                                name = "REF"
                                args = "pert=" + str(ivar) + ";name=" + name + ";ivar=0"
                                logging.debug("args: %s", args)
                                variables = scheduler.EcflowSuiteVariable("ARGS", args)

                                pert = scheduler.EcflowSuiteFamily(name, perturbations,
                                                                   variables=variables)
                                scheduler.EcflowSuiteTask("PerturbedRun", pert, ecf_files=ecf_files,
                                                          triggers=triggers)
                            if val == 1:
                                name = names[ivar]
                                args = f"pert={str(ivar + 1)};name={name};ivar={str(nivar)}"
                                logging.debug("args: %s", args)
                                variables = scheduler.EcflowSuiteVariable("ARGS", args)
                                pert = scheduler.EcflowSuiteFamily(name, perturbations,
                                                                   variables=variables)
                                scheduler.EcflowSuiteTask("PerturbedRun", pert, ecf_files=ecf_files,
                                                          triggers=triggers)
                                nivar = nivar + 1

                    prepare_oi_soil_input = None
                    prepare_oi_climate = None
                    if exp.config.setting_is("SURFEX#ASSIM#SCHEMES#ISBA", "OI"):
                        prepare_oi_soil_input = scheduler.EcflowSuiteTask("PrepareOiSoilInput",
                                                                          initialization,
                                                                          ecf_files=ecf_files)
                        prepare_oi_climate = scheduler.EcflowSuiteTask("PrepareOiClimate",
                                                                       initialization,
                                                                       ecf_files=ecf_files)

                    prepare_sst = None
                    if exp.config.setting_is("SURFEX#ASSIM#SCHEMES#SEA", "INPUT"):
                        if exp.config.setting_is("SURFEX#ASSIM#SEA#CFILE_FORMAT_SST", "ASCII"):
                            prepare_sst = scheduler.EcflowSuiteTask("PrepareSST", initialization,
                                                                    ecf_files=ecf_files)

                    an_variables = {"t2m": False, "rh2m": False, "sd": False}
                    obs_types = exp.config.get_setting("SURFEX#ASSIM#OBS#COBS_M")
                    nnco = exp.config.get_setting("SURFEX#ASSIM#OBS#NNCO")
                    for t_ind, val in enumerate(obs_types):
                        if nnco[t_ind] == 1:
                            if obs_types[t_ind] == "T2M" or obs_types[t_ind] == "T2M_P":
                                an_variables.update({"t2m": True})
                            elif obs_types[t_ind] == "HU2M" or obs_types[t_ind] == "HU2M_P":
                                an_variables.update({"rh2m": True})
                            elif obs_types[t_ind] == "SWE":
                                if do_snow_ass:
                                    an_variables.update({"sd": True})

                    analysis = scheduler.EcflowSuiteFamily("Analysis", initialization)
                    fg4oi = scheduler.EcflowSuiteTask("FirstGuess4OI", analysis,
                                                      ecf_files=ecf_files)
                    fg4oi_complete = scheduler.EcflowSuiteTrigger(fg4oi)

                    triggers = []
                    for var, active in an_variables.items():
                        if active:
                            an_var_fam = scheduler.EcflowSuiteFamily(var, analysis)
                            qc_triggers = None
                            if var == "sd":
                                qc_triggers = scheduler.EcflowSuiteTriggers(fg4oi_complete)
                            qc_task = scheduler.EcflowSuiteTask("QualityControl", an_var_fam,
                                                                triggers=qc_triggers,
                                                                ecf_files=ecf_files)
                            oi_triggers = scheduler.EcflowSuiteTriggers([
                                scheduler.EcflowSuiteTrigger(qc_task),
                                scheduler.EcflowSuiteTrigger(fg4oi)])
                            scheduler.EcflowSuiteTask("OptimalInterpolation", an_var_fam,
                                                      triggers=oi_triggers,
                                                      ecf_files=ecf_files)
                            triggers.append(scheduler.EcflowSuiteTrigger(an_var_fam))

                    oi2soda_complete = None
                    if len(triggers) > 0:
                        triggers = scheduler.EcflowSuiteTriggers(triggers)
                        oi2soda = scheduler.EcflowSuiteTask("Oi2soda", analysis, triggers=triggers,
                                                            ecf_files=ecf_files)
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
                        prepare_lsm = scheduler.EcflowSuiteTask("PrepareLSM", initialization,
                                                                ecf_files=ecf_files,
                                                                triggers=triggers)

                    triggers = [scheduler.EcflowSuiteTrigger(fg_task), oi2soda_complete]
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
                    scheduler.EcflowSuiteTask("Soda", analysis, triggers=triggers,
                                              ecf_files=ecf_files)

            triggers = scheduler.EcflowSuiteTriggers([scheduler.EcflowSuiteTrigger(cycle_input),
                                                      scheduler.EcflowSuiteTrigger(initialization)])
            prediction = scheduler.EcflowSuiteFamily("Prediction", dtg_node, triggers=triggers)
            prediction_dtg_node.update({dtg_str: prediction})

            forecast = scheduler.EcflowSuiteTask("Forecast", prediction, ecf_files=ecf_files)
            triggers = scheduler.EcflowSuiteTriggers(scheduler.EcflowSuiteTrigger(forecast))
            scheduler.EcflowSuiteTask("LogProgress", prediction,
                                      triggers=triggers,
                                      ecf_files=ecf_files)

            triggers = scheduler.EcflowSuiteTriggers(scheduler.EcflowSuiteTrigger(prediction))
            pp_fam = scheduler.EcflowSuiteFamily("PostProcessing", dtg_node, triggers=triggers)
            post_processing_dtg_node.update({dtg_str: pp_fam})

            log_pp_trigger = None
            if analysis is not None:
                qc2obsmon = scheduler.EcflowSuiteTask("Qc2obsmon", pp_fam, ecf_files=ecf_files)
                trigger = scheduler.EcflowSuiteTrigger(qc2obsmon)
                log_pp_trigger = scheduler.EcflowSuiteTriggers(trigger)

            scheduler.EcflowSuiteTask("LogProgressPP", pp_fam, triggers=log_pp_trigger,
                                      ecf_files=ecf_files)

            prev_dtg = dtg

        hours_behind = 24
        for dtg in dtgs:
            dtg_str = dtg.strftime("%Y%m%d%H%M")
            pp_dtg_str = (dtg - timedelta(hours=hours_behind)).strftime("%Y%m%d%H%M")
            if pp_dtg_str in post_processing_dtg_node:
                triggers = scheduler.EcflowSuiteTriggers(
                    scheduler.EcflowSuiteTrigger(post_processing_dtg_node[pp_dtg_str]))
                cycle_input_dtg_node[dtg_str].add_part_trigger(triggers)

    def save_as_defs(self, def_file):
        """Save definition file.

        Args:
            def_file (_type_): _description_
        """
        logging.debug("SurfexSuiteDefinition: Saving def file %s", def_file)
        self.suite.save_as_defs(def_file)


def get_defs(exp, system, progress, suite_type):
    """Get the definitions.

    Args:
        exp (experiment.Exp): Experiment
        system (experiment.System): System
        progress (experiment.Progress): DTG handling
        suite_type (str): What kind of suite

    Raises:
        Exception: _description_
        NotImplementedError: _description_

    Returns:
        scheduler.SuiteDefinition: A suite definitition
    """
    suite_name = exp.name
    logging.debug("Get defs for %s", suite_name)
    joboutdir = system.get_var("JOBOUTDIR", "0")
    env_submit = exp.work_dir + "/Env_submit"
    hh_list = exp.config.get_total_unique_cycle_list()
    dtgstart = progress.dtg
    dtgbeg = progress.dtgbeg
    dtgend = progress.dtgend
    logging.debug("%s: DTGSTART: %s DTGBEG: %s DTGEND: %s", __file__, dtgstart, dtgbeg, dtgend)
    if dtgbeg is None:
        dtgbeg = dtgstart
    dtgs = []
    dtg = dtgstart
    logging.debug("Building list of DTGs")
    while dtg <= dtgend:
        dtgs.append(dtg)
        # hour = dtg.strftime("%H")
        fcint = exp.config.get_fcint(dtg)
        logging.debug("DTG: %s, unique HH_LIST: %s. FCINT: %s", str(dtg), str(hh_list), fcint)
        # if len(hh_list) > 1:
        #    for h in range(0, len(hh_list)):
        #        logging.debug("%s %s %s", h, hh_list[h], hour)
        #        if int(hh_list[h]) == int(hour):
        #            if h == len(hh_list) - 1:
        #                fcint = ((int(hh_list[len(hh_list) - 1]) % 24) - int(hh_list[0])) % 24
        #            else:
        #                fcint = int(hh_list[h + 1]) - int(hh_list[h])
        # else:
        #    fcint = 24
        # if fcint is None:
        #    raise Exception
        dtg = dtg + timedelta(seconds=fcint)
    if suite_type == "surfex":
        return SurfexSuite(suite_name, exp, joboutdir, env_submit, dtgs, dtg, dtgbeg=dtgbeg)
    raise NotImplementedError(f"Suite definition for {suite_type} is not implemented!")
