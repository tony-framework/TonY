#!/bin/bash
###########################################################################################################
# NAME: buildAndDeploy.sh
#
# DESCRIPTION:
# This script will send the $PROJECT_NAME.zip generated after running `gradle dist` to host and user given
# given as inputs.
#
#
# INPUT:
# $1 - user
# $2 - hostname
#
#
# OUTPUT:
# $DEPLOY_LOG: log file that includes all tasks.
#
#
# ENVIRONMENT VARIABLES:
#
#
# NOTES:
# Run this script in tony-history-server/ folder.
#
#
# EXIT CODE:
# 0 - Success
# 1 - Cleaning step failed
# 2 - Bundling distribution zip step failed
# 3 - Copying to remote host failed
#
#
# CHANGELOG:
# OCT 24 2018 PHAT TRAN
# OCT 31 2018 PHAT TRAN - Added project name variable for copying task
############################################################################################################
DEPLOY_LOG=deploy.log
PROJECT_NAME=tony-history-server

echo "Cleaning up old builds..." | tee $DEPLOY_LOG
gradle clean 2>&1 | tee -a $DEPLOY_LOG
if [ $? -ne 0 ];
then
  exit 1
fi
echo | tee -a $DEPLOY_LOG

echo "Packaging distribution zip..." | tee -a $DEPLOY_LOG
gradle dist 2>&1 | tee -a $DEPLOY_LOG
if [ $? -ne 0 ];
then
  exit 2
fi
echo | tee -a $DEPLOY_LOG

echo "Copying over to $1@$2..." | tee -a $DEPLOY_LOG
scp build/distributions/${PROJECT_NAME}.zip $1@$2:~/. 2>&1 | tee -a $DEPLOY_LOG
if [ $? -ne 0 ];
then
  exit 3
fi
echo | tee -a $DEPLOY_LOG

echo "Deployed to $1@$2!" | tee -a $DEPLOY_LOG
exit 0
