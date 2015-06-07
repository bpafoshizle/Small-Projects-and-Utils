#!/bin/sh
#
#  Copyright 2006 Cognos Incorporated. All Rights Reserved.
#  Cognos and the Cognos logo are trademarks of Cognos Incorporated.
#
# Build the Java samples.

# REMOVE this when you edit this file.
# echo You MUST edit this file before you can compile this application.


# *  *  *  *  *  *  *  *
# CHANGE the following environment variables to point to the
# Java Development Kit and Cognos 8 on your system.
# *  *  *  *  *  *  *  *

# Edited by Barret Miller to set CRN_HOME and JAVA_HOME environment
# variables to correct locations

if [ "$CRN_HOME" = "" ] ; then
	CRN_HOME=/cognos/performcentral
fi
if [ "$JAVA_HOME" = "" ] ; then
	JAVA_HOME=/cognos/java5
fi

JAR_HOME=$CRN_HOME/sdk/java/lib

JAVAC=$JAVA_HOME/bin/javac

# Build the CLASSPATH required to build the Java samples

CLASSPATH=$JAVA_HOME/lib/tools.jar
for jar in axis axisCognosClient commons-discovery commons-logging \
	dom4j jaxrpc saaj xalan xml-apis xercesImpl; do
  CLASSPATH="$CLASSPATH:$JAR_HOME/$jar.jar"
done
CLASSPATH="$CLASSPATH:./:../Common:../Security:../ReportParams:../HandlerCS:../ReportSpec:../ViewCMReports:../ViewCMPackages:../ExecReports:../runreport:../Scheduler:../ContentStoreExplorer"
export CLASSPATH

# Compile

$JAVAC -classpath "$CLASSPATH" *.java
