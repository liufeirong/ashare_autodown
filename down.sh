#!/bin/bash
ROOT=/Users/harry/Workspace/AlphagoTrader
PYTHON=/Users/harry/anaconda/envs/py36/bin/python
cur_date=`date +%Y-%m-%d`
${PYTHON} ${ROOT}/down.py >> ${ROOT}/logs/${cur_date}-down_log.txt 
