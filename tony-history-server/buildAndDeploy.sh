#!/bin/bash

DEPLOY_LOG=deploy.log

echo -n "Cleaning up old builds..." > $DEPLOY_LOG
echo -n "Cleaning up old builds..." 
gradle clean 2>&1 >> $DEPLOY_LOG
echo >> $DEPLOY_LOG
echo

echo -n "Packaging distribution zip..." >> $DEPLOY_LOG
echo -n "Packaging distribution zip..."
gradle dist 2>&1 >> $DEPLOY_LOG
echo >> $DEPLOY_LOG
echo

echo -n "Copying over to $1..." >> $DEPLOY_LOG
echo -n "Copying over to $1..."
scp build/distributions/playBinary.zip phattran@$1:~/. 2>&1 >> $DEPLOY_LOG
echo >> $DEPLOY_LOG
echo

echo "Deployed to $1!" >> $DEPLOY_LOG
echo "Deployed to $1!"
