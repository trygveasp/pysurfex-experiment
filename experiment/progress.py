import os
from datetime import datetime, timedelta
import json


class Progress(object):
    def __init__(self, progress, progress_pp):
        """
        Initialize the experiment progress

        Args:
            progress (dict): Contains the current date/time information
            progress_pp (dict): Contains the current date/time information for psot-processing
        """

        # Update DTG/DTGBED/DTGEND
        if "DTG" in progress:
            dtg = progress["DTG"]
            # Dump DTG to progress
            if "DTGEND" in progress:
                dtgend = progress["DTGEND"]
            else:
                if "DTGEND" in progress:
                    dtgend = progress["DTGEND"]
                else:
                    dtgend = progress["DTG"]

            if "DTGBEG" in progress:
                dtgbeg = progress["DTGBEG"]
            else:
                if "DTG" in progress:
                    dtgbeg = progress["DTG"]
                else:
                    raise Exception("Can not set DTGBEG")
            if dtgbeg is not None:
                if isinstance(dtgbeg, str):
                    dtgbeg = datetime.strptime(dtgbeg, "%Y%m%d%H")
                self.dtgbeg = dtgbeg
            else:
                self.dtgbeg = None
            if dtg is not None:
                if isinstance(dtg, str):
                    dtg = datetime.strptime(dtg, "%Y%m%d%H")
                self.dtg = dtg
            else:
                self.dtg = None
            if dtgend is not None:
                if isinstance(dtgend, str):
                    dtgend = datetime.strptime(dtgend, "%Y%m%d%H")
                self.dtgend = dtgend
            else:
                self.dtgend = None
        else:
            raise Exception

        # Update DTGPP
        dtgpp = None
        if "DTGPP" in progress_pp:
            dtgpp = progress_pp["DTGPP"]
        elif "DTG" in progress:
            dtgpp = progress["DTG"]
        if dtgpp is not None:
            if isinstance(dtgpp, str):
                dtgpp = datetime.strptime(dtgpp, "%Y%m%d%H")
            self.dtgpp = dtgpp

        print("DTGEND", self.dtgend)

    def export_to_file(self, fname):
        fh = open(fname, "w")
        fh.write("export DTG=" + self.dtg.strftime("%Y%m%d%H") + "\n")
        fh.write("export DTGBEG=" + self.dtgbeg.strftime("%Y%m%d%H") + "\n")
        fh.write("export DTGEND=" + self.dtgend.strftime("%Y%m%d%H") + "\n")
        fh.write("export DTGPP=" + self.dtgpp.strftime("%Y%m%d%H") + "\n")
        fh.close()

    # Members could potentially have different DTGBEGs
    def get_dtgbeg(self, fcint):
        """
        get the first DTG of the run

        Args:
            fcint (int): Time in hours between the forecasts/analysis

        Returns:
            dtgbeg (datetime.datetime):  first DTG of the run

        """

        dtgbeg = self.dtgbeg
        if (self.dtg - timedelta(hours=int(fcint))) < self.dtgbeg:
            dtgbeg = self.dtg
        return dtgbeg

    # Members could potentially have different DTGENDs
    def get_dtgend(self, fcint):
        dtgend = self.dtgend
        if self.dtgend < (self.dtg + timedelta(hours=int(fcint))):
            dtgend = self.dtg
        return dtgend

    def increment_progress(self, fcint_min, pp=False):
        if pp:
            self.dtgpp = self.dtgpp + timedelta(hours=fcint_min)
        else:
            self.dtg = self.dtg + timedelta(hours=fcint_min)
            if self.dtgend < self.dtg:
                self.dtgend = self.dtg

    def save(self, progress_file, progress_pp_file, log=True, log_pp=True, indent=None):
        progress = {
            "DTGBEG": self.dtgbeg.strftime("%Y%m%d%H"),
            "DTG": self.dtg.strftime("%Y%m%d%H")
        }
        progress_pp = {
            "DTGPP": self.dtgpp.strftime("%Y%m%d%H"),
        }
        if log:
            json.dump(progress, open(progress_file, "w"), indent=indent)
        if log_pp:
            json.dump(progress_pp, open(progress_pp_file, "w"), indent=indent)


class ProgressFromFile(Progress):
    def __init__(self, progress_file, progress_pp_file):
        """
        Initialize a progress object from files

        Args:
            progress_file (str): Full path of the progress files
            progress_pp_file (str): Full path of the post-processing progress file
        """

        self.progress_file = progress_file
        self.progress_pp_file = progress_pp_file
        if os.path.exists(self.progress_file):
            progress = json.load(open(self.progress_file, "r"))
        else:
            progress = {
                "DTGBEG": None,
                "DTG": None,
                "DTGEND": None
            }
        if os.path.exists(self.progress_pp_file):
            progress_pp = json.load(open(self.progress_pp_file, "r"))
        else:
            progress_pp = {
                "DTGPP": None
            }

        Progress.__init__(self, progress, progress_pp)

    def increment_progress(self, fcint_min, pp=False, indent=None):
        Progress.increment_progress(self, fcint_min, pp=False)
        if pp:
            updated_progress_pp = {
                "DTGPP": self.dtgpp.strftime("%Y%m%d%H")
            }
            json.dump(updated_progress_pp, open(self.progress_pp_file, "w"))
        else:
            updated_progress = {
                "DTGBEG": self.dtgbeg.strftime("%Y%m%d%H"),
                "DTG": self.dtg.strftime("%Y%m%d%H"),
                "DTGEND": self.dtgend.strftime("%Y%m%d%H")
            }
            json.dump(updated_progress, open(self.progress_file, "w"), indent=indent)
