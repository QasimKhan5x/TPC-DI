# TPC-DI

Contains implementation of TPC-DI Benchmark V1.1.0 using Python for ETL into MySQL.

## Usage

Ensure you have installed the latest version of MySQL Community Server. Then, create a new environment, preferably with `conda`, and install the requisite packages: 

    conda create -n tpcdi python=3.8 -y
    conda activate tpcdi
    pip install -r requirements.txt

First, create a MySQL database for the required scale factor database. It should be called `tpcdi_sf5` or `tpcdi_sf3` or similar. 

The entire implementation can be found `etl.ipynb`. The notebook provides an interactive execution of the notebook. Otherwise, to run the benchmark in a command line, run the following:

    python -m scripts.historical
    python -m scripts.incremental

Running the scripts allows measuring the elapsed time for each phase.
