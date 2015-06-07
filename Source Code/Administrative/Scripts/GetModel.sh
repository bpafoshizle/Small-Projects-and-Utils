#!/bin/sh
#
#  Copyright 2006 Cognos Incorporated. All Rights Reserved.
#  Cognos and the Cognos logo are trademarks of Cognos Incorporated.
#
# Run the samples in GUI mode.

# CHANGE the following environment variables to point to Cognos 8 on your system.

if [ "$CRN_HOME" = "" ] ; then
  CRN_HOME=/cognos/dev/performcentral
fi
if [ "$JAVA_HOME" = "" ] ; then
  JAVA_HOME=/cognos/dev/java5/jre
fi

JAVA=/cognos/dev/java5/bin/java
JAR_HOME=$CRN_HOME/sdk/java/lib

# Build the CLASSPATH required

CLASSPATH=
for dir in ../Common ../HandlersCS ../Security ../ReportParams ../ReportSpec ../ViewCMReports ../ViewCMPackages ../ExecReports ../runreport ../Scheduler ../ContentStoreExplorer;
 do
  CLASSPATH="$CLASSPATH:$dir"
done
for jar in axis axisCognosClient commons-discovery commons-logging \
  dom4j jaxrpc saaj xalan xml-apis xercesImpl ; do
  CLASSPATH="$CLASSPATH:$JAR_HOME/$jar.jar"
done

# Run SecurityUI.java
$JAVA -classpath "$CLASSPATH" GetModel $1