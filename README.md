# TPC-DI

This repository contains the implementation of TPC-DI Benchmark V1.1.0 using Python for ETL into MySQL.

## Usage

Ensure you have installed the latest version of MySQL Community Server. Then, create a new environment, preferably with `conda`, and install the requisite packages: 

    conda create -n tpcdi python=3.10 -y
    conda activate tpcdi
    pip install -r requirements.txt

First, create a MySQL database for the required scale factor database. It should be called `tpcdi_sf5` or `tpcdi_sf3` or similar. 

The entire implementation can be found `etl.ipynb`. The notebook provides an interactive execution of the benchmark phases. Otherwise, to run the benchmark in a command line, run the following:

    python -m scripts.historical
    python -m scripts.incremental

Running the scripts allows measuring the elapsed time for each phase.

To prepare the `Audit` table for the automated audit phase, run the following:

    python -m scripts.audit

Run the automated audit by running `validation/tpcdi_audit.sql`. If all tests pass, then the benchmark has been implemented correctly.

## Future Work

- [ ] Prevent deadlocks in running visibility queries during incremental updates
- [ ] Ensure all tests pass in audit

