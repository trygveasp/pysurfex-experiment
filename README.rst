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

 # Alternative way of setting up a pre-defined SEKF configuration
 surfExp my_config.toml SEKF experiment/data/config data/config/configurations/sekf.toml
 
 # Use AROME Arctic branch on PPI together with MET-Norway LDAS
 surfExp my_config.toml LDAS experiment/data/config/configurations/metno_ldas.toml experiment/data/config/mods/arome_arctic_offline_ppi.toml

 # To start you experiment
 deode start suite --config-file my_config.toml



Here is an example with CARRA2 using poetry run.

.. code-block:: bash
 cd ~/projects/pysurfex-experiment

 # Create experiment in file data/config/CARRA2_MINI.toml
 poetry run surfExp CARRA2_MINI.toml CARRA2-MINI experiment/data/config/configurations/carra2.toml

 # Modify times in data/config/CARRA2_MINI.toml
 # Run experiment from config file data/config/CARRA2_MINI.toml
 poetry run deode start suite --config-file data/config/CARRA2_MINI.toml


Extra environment on PPI-RHEL8 needed to start experiments
---------------------------------------------------------------

.. code-block:: bash

 # ib-dev queue is only in A: ib-dev-a-r8.q
 ssh ppi-r8login-a1.int.met.no
 
 # Get pysurfex-experiment
 git clone github.com:trygveasp/pysurfex-experiment.git  --branch feature/deode_offline_surfex pysurfex_experiment

 # conda setup
 source /modules/rhel8/user-apps/suv-modules/miniconda3/24.7.1/etc/profile.d/conda.sh
 conda create -n pysurfex_experiment python==3.10 -y
 conda install -c conda-forge -n pysurfex_experiment poetry gdal -y
 conda activate pysurfex_experiment
 
 # Install
 poetry install
 
 # MET-Norway LDAS experiment
 mkdir -f exps
 surfExp exps/LDAS.toml LDAS $PWD/data/config/configurations/metno_ldas.toml $PWD/data/config/domains/MET_NORDIC_1_0.toml $PWD/data/config/mods/arome_arctic_offline_ppi.toml $PWD/data/config/mods/netcdf_input_pgd.toml $PWD/data/config/scheduler/ecflow_ppi_rhel8-$USER.toml

 # PPI ECFLOW (in A)
 # If your server is not running you should start it!
 module use /modules/MET/rhel8/user-modules/
 module load ecflow/5.8.1
 export ECF_SSL=1

 # Start suite (modify dates)
 deode start suite --config-file exps/LDAS.toml


 # MET-Norway LDAS single decade
 surfExp exps/LDAS_decade.toml LDAS_decade $PWD/data/config/configurations/metno_ldas.toml $PWD/data/config/domains/MET_NORDIC_1_0.toml $PWD/data/config/mods/arome_arctic_offline_ppi.toml $PWD/data/config/mods/netcdf_input_pgd.toml $PWD/data/config/mods/netcdf_input_single_decade.toml $PWD/data/config/mods/metno_ldas_single_decade.toml $PWD/data/config/scheduler/ecflow_ppi_rhel8-$USER.toml
 deode start suite  --config-file exps/LDAS_decade.toml

Trainings
-----------------------

`Budapest May 2022 <https://github.com/metno/pysurfex-experiment/blob/master/trainings/budapest_may_2022.rst/>`_ (Old version)
