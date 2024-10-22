.. SURFEX Python API documentation master file, created by
   sphinx-quickstart on Mon Mar  2 18:25:38 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

surfExp documentation
===================================================================

.. toctree::
   :maxdepth: 3
   :caption: Contents:

.. include::  README.rst

Classes
-------------------------------------------------------------------------------
.. autoclass:: surfexp.tasks.tasks.PySurfexBaseTask
.. autoclass:: surfexp.tasks.surfex_binary_task.SurfexBinaryTask
.. autoclass:: surfexp.tasks.surfex_binary_task.OfflinePgd
.. autoclass:: surfexp.tasks.surfex_binary_task.OfflinePrep
.. autoclass:: surfexp.tasks.surfex_binary_task.OfflineForecast
.. autoclass:: surfexp.tasks.surfex_binary_task.PerturbedRun
.. autoclass:: surfexp.tasks.surfex_binary_task.Soda
.. autoclass:: surfexp.tasks.tasks.PrepareCycle
.. autoclass:: surfexp.tasks.tasks.QualityControl
.. autoclass:: surfexp.tasks.tasks.OptimalInterpolation
.. autoclass:: surfexp.tasks.forcing.Forcing
.. autoclass:: surfexp.tasks.tasks.FirstGuess
.. autoclass:: surfexp.tasks.tasks.CycleFirstGuess
.. autoclass:: surfexp.tasks.tasks.Oi2soda
.. autoclass:: surfexp.tasks.tasks.Qc2obsmon
.. autoclass:: surfexp.tasks.tasks.FirstGuess4OI
.. autoclass:: surfexp.tasks.compilation.MakeOfflineBinaries
.. autoclass:: surfexp.tasks.compilation.SyncSourceCode
.. autoclass:: surfexp.tasks.compilation.ConfigureOfflineBinaries
.. autoclass:: surfexp.tasks.compilation.CMakeBuild


Class methods
---------------------------------------------
.. automethod:: surfexp.tasks.tasks.PySurfexBaseTask.__init__
.. automethod:: surfexp.tasks.tasks.PySurfexBaseTask.run
.. automethod:: surfexp.tasks.tasks.PySurfexBaseTask.execute

#.. automethod:: surfexp.tasks.AbstractTask.postfix
#.. automethod:: surfexp.tasks.SurfexBinaryTask.__init__
##.. automethod:: surfexp.tasks.SurfexBinaryTask.execute
#.. automethod:: surfexp.tasks.Pgd.__init__
#.. automethod:: surfexp.tasks.Pgd.execute
#.. automethod:: surfexp.tasks.Prep.__init__
#.. automethod:: surfexp.tasks.Prep.execute
#.. automethod:: surfexp.tasks.Forecast.__init__
#.. automethod:: surfexp.tasks.Forecast.execute
#.. automethod:: surfexp.tasks.PerturbedRun.__init__
#.. automethod:: surfexp.tasks.PerturbedRun.execute
#.. automethod:: surfexp.tasks.Soda.__init__
#.. automethod:: surfexp.tasks.Soda.execute
#.. automethod:: surfexp.tasks.Soda.postfix
#.. automethod:: surfexp.tasks.PrepareCycle.__init__
#.. automethod:: surfexp.tasks.PrepareCycle.run
#.. automethod:: surfexp.tasks.PrepareCycle.execute
#.. automethod:: surfexp.tasks.QualityControl.__init__
#.. automethod:: surfexp.tasks.QualityControl.execute
#.. automethod:: surfexp.tasks.OptimalInterpolation.__init__
#.. automethod:: surfexp.tasks.OptimalInterpolation.execute
#.. automethod:: surfexp.tasks.Forcing.__init__
#.. automethod:: surfexp.tasks.Forcing.execute
#.. automethod:: surfexp.tasks.FirstGuess.__init__
#.. automethod:: surfexp.tasks.FirstGuess.execute
#.. automethod:: surfexp.tasks.CycleFirstGuess.__init__
#.. automethod:: surfexp.tasks.CycleFirstGuess.execute
#.. automethod:: surfexp.tasks.Oi2soda.__init__
#.. automethod:: surfexp.tasks.Oi2soda.execute
#.. automethod:: surfexp.tasks.Qc2obsmon.__init__
#.. automethod:: surfexp.tasks.Qc2obsmon.execute
#.. automethod:: surfexp.tasks.FirstGuess4OI.__init__
#.. automethod:: surfexp.tasks.FirstGuess4OI.execute
#.. automethod:: surfexp.tasks.FirstGuess4OI.write_file
#.. automethod:: surfexp.tasks.MakeOfflineBinaries.__init__
#.. automethod:: surfexp.tasks.MakeOfflineBinaries.execute

Methods
---------------------------------------------
.. autofunction:: surfexp.templates.cli.execute_task
.. autofunction:: surfexp.cli.pysfxexp
.. autofunction:: surfexp.experiment.get_nnco
.. autofunction:: surfexp.experiment.get_total_unique_cycle_list
.. autofunction:: surfexp.experiment.get_cycle_list
.. autofunction:: surfexp.experiment.get_setting
.. autofunction:: surfexp.experiment.setting_is
.. autofunction:: surfexp.experiment.get_fgint


* :ref: `README`

Indices and tables
==================

* :ref:`genindex`
* :ref:`search`


