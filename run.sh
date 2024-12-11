#!/bin/bash
cd proj_24-25-p1_base

make

cd ..

cd jobs
python3 generator.py

../proj_24-25-p1_base//kvs ../jobs 5 10