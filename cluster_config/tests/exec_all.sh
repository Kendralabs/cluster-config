#!/bin/bash

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")


echo $SCRIPTPATH


nosetests --with-coverage --cover-package=cluster_config --cover-erase --cover-inclusive --cover-html --with-xunit --xunit-file=$SCRIPTPATH/nosetests.xml

success=$?

if [[ $success == 0 ]] ; then
   echo "Python Tests Successful"
   exit 0
fi

