#!/usr/bin/env sh
###########################################################################################################
# NAME: stopTonyPortal.sh
#
# DESCRIPTION:
# This script stops the Tony Portal process.
#
#
# INPUT:
# $1 - path of RUNNING_PID of TonY Portal (Optional. Default to current folder)
#
#
# EXIT CODE:
# 0 - Success
# 1 - Failed to find RUNNING_PID
#
#
# CHANGELOG:
# DEC 10 2018 PHAT TRAN
############################################################################################################
RUNNING_PID_PATH=./RUNNING_PID
if [ ! -z "$1" ]; then
    RUNNING_PID_PATH=$1
fi

PID=`cat $RUNNING_PID_PATH`
if [ $? -ne 0 ]; then
    echo "Invalid path to RUNNING_PID"
    exit 1
fi

kill -9 $PID

rm $RUNNING_PID_PATH # So we can run startTonyPortal.sh again
