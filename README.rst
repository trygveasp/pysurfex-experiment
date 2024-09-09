.. _README:

.. image:: https://coveralls.io/repos/github/metno/pysurfex-experiment/badge.svg?branch=master

https://coveralls.io/github/metno/pysurfex-experiment


This repository is a setup to create and run offline SURFEX experiments.
=========================================================================

See online documentation in https://metno.github.io/pysurfex-experiment/
The setup is dependent of pysurfex (https://metno.github.io/pysurfex)

You need a python3 parser and the following dependencies are needed. Install the non-standard ones e.g. with pip or your system installation system. Requirements can be found in https://github.com/metno/pysurfex-experiment/blob/master/requirements.txt


General dependencies (from pypi)
---------------------------------

.. code-block:: bash

 pysurfex

pysurfex bring extra dependencies like gridpp, titanlib. If you install from pypi with pip these should be handled automatically.

Installation
-------------

The recommended installation method is using poetry. First install poetry. This can be done in several ways like a system package or ftom pypi. The recommended way is:

.. code-block:: bash

 curl -sSL https://install.python-poetry.org | python3 -


When you have poetry installed make sure you have it in path. It might be installed in ~/.local/bin.
To install the script system first clone https://github.com/metno/pysurfex-experiment and install it.
To run commands interactively in the poetry environment you need to run either "poetry shell" or "poetry run [cmd]"


.. code-block:: bash

 cd
 mkdir -p projects
 cd projects

 # Clone the source code
 clone https://github.com/metno/pysurfex-experiment

 # Install the script system
 cd pysurfex-experiment
 poetry install

 # Enter the environment
 poetry shell


Usage
---------------------------------------------

The altering of the configuration must then be done by applying a defined configuration or a configuration file with patches from original configuration. Examples are local configurations, domains etc.
In addition you will get some other config files used in the tasks. An example on how to use it inside a poetry environment ("poetry shell")

.. code-block:: bash

 # First make sure you are in a poetry environment after executing "poetry shell"
 cd ~/projects/pysurfex-experiment
 poetry shell

 
 # The -offline argument is optional if you want to run with existing binaries
 PySurfexExpSetup --config-file data/config/my_config.toml
 # This will create a file exp_dependencies.json

 # Alternative way of setting up a pre-defined SEKF configuration
 PySfxExp $PWD/data/config data/config/configurations/sekf.toml --config-file data/config/my_config.toml
 
 # Use AROME Arctic branch on PPI together with MET-Norway LDAS
 PySfxExp data/config/configurations/metno_ldas.toml data/config/mods/arome_arctic_offline_ppi.toml --config-file data/config/my_config.toml

 # To start you experiment
 deode start suite --config-file data/config/my_config.toml



Here is an example with CARRA2 using poetry run.

.. code-block:: bash
 cd ~/projects/pysurfex-experiment

 # Create experiment in file data/config/CARRA2_MINI.toml
 PySfxExp --config-file data/config/deode.toml --output data/config/CARRA2_MINI.toml --case-name CARRA2-MINI $PWD/data/config data/config/configurations/carra2.toml --case-name CARRA2-MINI

 # Modify times in data/config/CARRA2_MINI.toml
 # Run experiment from config file data/config/CARRA2_MINI.toml
 poetry run deode start suite --config-file data/config/CARRA2_MINI.toml


Extra environment on PPI-RHEL8 needed to start experiments
---------------------------------------------------------------

.. code-block:: bash

 module use /modules/MET/rhel8/user-modules/
 module load ecflow/5.8.1
 export ECF_SSL=1

 source /modules/rhel8/user-apps/suv-modules/miniconda3/24.7.1/etc/profile.d/conda.sh
 conda activate pysurfex_experiment

 # MET-Norway LDAS
 PySfxExp --config-file data/config/deode.toml --output data/config/LDAS_AA.toml --case-name LDAS --config-dir $PWD/data/config data/config/configurations/metno_ldas.toml data/config/include/domains/MET_NORDIC_2_5.toml data/config/mods/arome_arctic_offline_ppi.toml --case-name LDAS_AA

Trainings
-----------------------

`Budapest May 2022 <https://github.com/metno/pysurfex-experiment/blob/master/trainings/budapest_may_2022.rst/>`_ (Old version)
