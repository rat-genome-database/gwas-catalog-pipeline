#!/usr/bin/env bash
#
# GWAS Catalog import pipeline
#
. /etc/profile
APPNAME=GwasCatalogPipeline
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`

APPDIR=/home/rgddata/pipelines/$APPNAME
cd $APPDIR

java -Dspring.config=$APPDIR/../properties/default_db2.xml \
    -Dlog4j.configuration=file://$APPDIR/properties/log4j.properties \
    -jar lib/$APPNAME.jar --importAssoc"$@" > run.log 2>&1

mailx -s "[$SERVER] GWAS Catalog Pipeline Run" mtutaj@mcw.edu,llamers@mcw.edu < $APPDIR/logs/summary.log
