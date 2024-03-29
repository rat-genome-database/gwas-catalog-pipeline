#!/usr/bin/env bash
#
# GWAS Catalog import pipeline
#
. /etc/profile
APPNAME="gwas-catalog-pipeline"
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`

APPDIR=/home/rgddata/pipelines/$APPNAME
cd $APPDIR

java -Dspring.config=$APPDIR/../properties/default_db2.xml \
    -Dlog4j.configurationFile=file://$APPDIR/properties/log4j2.xml \
    -jar lib/$APPNAME.jar --removeDupes"$@" > runDupeRmv.log 2>&1

mailx -s "[$SERVER] GWAS Catalog Pipeline Run" mtutaj@mcw.edu,llamers@mcw.edu < $APPDIR/logs/dupeRemove.log
