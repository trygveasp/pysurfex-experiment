"""Handling of date/time information."""
import os
from datetime import datetime
import logging
import json


class Progress():
    """
    Progress.

    For internal use in experiment on HOST0.

    """

    def __init__(self, dtg, dtgbeg, dtgend=None, dtgpp=None):
        """Initialize the experiment progress.

        Args:
            dtg (datetime.datetime): current date/time information
            dtgbeg (datetime.datetime): first date/time information
            dtgend (datetime.datetime): last date/time information
            dtgpp (datetime.datetime): current post-processing date/time information

        """
        # Update DTG
        self.dtg = dtg
        if self.dtg is None:
            self.dtg_string = None
        else:
            self.dtg_string = self.dtg.strftime("%Y%m%d%H")

        # Update DTGEND
        self.dtgend = dtgend
        if dtgend is None:
            self.dtgend = self.dtg
        if self.dtgend is None:
            self.dtgend_string = None
        else:
            self.dtgend_string = self.dtgend.strftime("%Y%m%d%H")

        # Update DTGBEG
        self.dtgbeg = dtgbeg
        if dtgbeg is None:
            self.dtgbeg_string = None
        else:
            self.dtgbeg_string = self.dtgbeg.strftime("%Y%m%d%H")

        self.dtgpp = dtgpp
        if dtgpp is None:
            self.dtgpp = self.dtg
        if dtgpp is None:
            self.dtgpp_string = None
        else:
            self.dtgpp_string = self.dtgpp.strftime("%Y%m%d%H")

        logging.debug("DTG: %s", self.dtg_string)
        logging.debug("DTGBEG: %s", self.dtgbeg_string)
        logging.debug("DTGEND: %s", self.dtgend_string)
        logging.debug("DTGPP: %s", self.dtgpp_string)

    def save(self, progress_file, progress_pp_file, indent=None):
        """Save progress to file.

        Args:
            progress_file (str): Progress file
            progress_pp_file (str): Post-processing progress file
            log (bool, optional): Write progress file. Defaults to True.
            log_pp (bool, optional): Write post-processing logfile. Defaults to True.
            indent (int, optional): Indentation in json file. Defaults to None.

        """
        progress = {
            "DTGBEG": self.dtgbeg_string,
            "DTG": self.dtg_string,
            "DTGEND": self.dtgend_string
        }
        progress_pp = {
            "DTGPP": self.dtgpp_string,
        }

        logging.debug("progress file: %s", progress_file)
        logging.debug("progress_pp: %s", progress_pp_file)
        json.dump(progress, open(progress_file, mode="w", encoding="UTF-8"), indent=indent)
        json.dump(progress_pp, open(progress_pp_file, mode="w", encoding="UTF-8"),
                      indent=indent)

class ProgressFromFile(Progress):
    """Create progress object form a json file."""

    def __init__(self, progress_file, progress_pp_file):
        """Initialize a progress object from files.

        Args:
            progress_file (str): Full path of the progress files
            progress_pp_file (str): Full path of the post-processing progress file

        """
        self.progress_file = progress_file
        self.progress_pp_file = progress_pp_file
        if os.path.exists(self.progress_file):
            with open(self.progress_file, mode="r",encoding="UTF-8") as file_handler:
                progress = json.load(file_handler)
                dtg = progress.get("DTG")
                if dtg is not None:
                    dtg = datetime.strptime(dtg, "%Y%m%d%H")
                dtgbeg = progress.get("DTGBEG")
                if dtgbeg is not None:
                    dtgbeg = datetime.strptime(dtgbeg, "%Y%m%d%H")
                dtgend = progress.get("DTGEND")
                if dtgend is not None:
                    dtgend = datetime.strptime(dtgend, "%Y%m%d%H")
        else:
            dtg = None
            dtgbeg = None
            dtgend = None
        dtgpp = None
        if os.path.exists(self.progress_pp_file):
            with open(self.progress_pp_file, mode="r",encoding="UTF-8") as file_handler:
                dtgpp = json.load(file_handler).get("DTGPP")
                if dtgpp is not None:
                    dtgpp = datetime.strptime(dtgpp, "%Y%m%d%H")
        else:
            dtgpp = None

        Progress.__init__(self, dtg, dtgbeg, dtgend=dtgend, dtgpp=dtgpp)

'''
    # Members could potentially have different DTGBEGs
    def get_dtgbeg(self, fcint):
        """Get the first DTG of the run.

        Args:
            fcint (int): Time in hours between the forecasts/analysis

        Returns:
            dtgbeg (datetime.datetime):  first DTG of the run

        """
        dtgbeg = self.dtgbeg
        if (self.dtg - timedelta(hours=int(fcint))) < self.dtgbeg:
            dtgbeg = self.dtg
        return dtgbeg

    def get_dtgend(self, fcint):
        """Get the last DTG.

        Members could potentially have different DTGENDs

        Args:
            fcint (int): Interval between forecasts

        Returns:
            datetime: Last DTG.

        """
        dtgend = self.dtgend
        if self.dtgend < (self.dtg + timedelta(hours=int(fcint))):
            dtgend = self.dtg
        return dtgend
'''
