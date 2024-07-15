#!/usr/bin/env bash
#
# GWAS Catalog import pipeline
#
. /etc/profile
APPNAME="gwas-catalog-pipeline"
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAILLIST=mtutaj@mcw.edu,llamers@mcw.edu
if [ "$SERVER" == "REED" ]; then
  EMAILLIST=llamers@mcw.edu,mtutaj@mcw.edu,jrsmith@mcw.edu,akwitek@mcw.edu
fi
APPDIR=/home/rgddata/pipelines/$APPNAME
cd $APPDIR

java -Dspring.config=$APPDIR/../properties/default_db2.xml \
    -Dlog4j.configurationFile=file://$APPDIR/properties/log4j2.xml \
    -jar lib/$APPNAME.jar --withdrawOldVars"$@" > run.log 2>&1

[ -s $APPDIR/logs/summary.log ] && mailx -s "[$SERVER] GWAS Catalog Pipeline Run" $EMAILLIST < $APPDIR/logs/summary.log