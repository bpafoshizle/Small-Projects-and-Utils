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

if [ "$CRN_HOME" = "" ] ; then
	CRN_HOME=/usr/cognos/c8
fi
if [ "$JAVA_HOME" = "" ] ; then
	JAVA_HOME=/c/j2sdk1.4.2
fi

JAR_HOME=$CRN_HOME/sdk/java/lib

JAVAC=$JAVA_HOME/bin/javac

# Build the CLASSPATH required to build the Java samples

CLASSPATH=$JAVA_HOME/lib/tools.jar
for jar in axis axisCognosClient commons-discovery commons-logging \
	jaxrpc dom4j xalan saaj xml-apis xercesImpl; do
  CLASSPATH="$CLASSPATH:$JAR_HOME/$jar.jar"
done
CLASSPATH="$CLASSPATH:../Common:../Security:../ReportParams:../HandlersCS:../ReportSpec:../ViewCMReports:../ViewCMPackages:../ExecReports:../runreport:../Scheduler:../ContentStoreExplorer"

# Compile

$JAVAC -classpath "$CLASSPATH" *.java

