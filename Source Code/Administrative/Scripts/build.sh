#!/bin/sh

#  Copyright 2006 Cognos Incorporated. All Rights Reserved.
#  Cognos and the Cognos logo are trademarks of Cognos Incorporated.

# Build Java files in directory ExecReports

# *  *  *  *  *  *  *  *
# CHANGE the following environment variables to point to the
# Java Development Kit and Cognos 8 on your system.
# *  *  *  *  *  *  *  *

if [ "$CRN_HOME" = "" ] ; then
	CRN_HOME=/usr/cognos/c8
fi

if [ "$JAVA_HOME" = "" ]; then
	JAVA_HOME="/c/jdk1.3.1_02"
fi
# Build the CLASSPATH required to build Java files in ExecReports

JAR_HOME="${CRN_HOME}/sdk/java/lib"
JAVAC="${JAVA_HOME}/bin/javac"

CLASSPATH="${JAR_HOME}/tools.jar"
for jar in axis axisCognosClient commons-discovery commons-logging jaxrpc \
        saaj xercesImpl xml-apis wsdl4j dom4j xalan ; do
  CLASSPATH="${CLASSPATH}:${JAR_HOME}/${jar}.jar"
done
CLASSPATH="${CLASSPATH}:${CRN_HOME}/tomcat4.1.27/common/lib/servlet.jar"
CLASSPATH="${CLASSPATH}:${CRN_HOME}/tomcat4.1.27/server/lib/log4j-1.2.8.jar"

# Compile Java files
${JAVAC} -classpath ${CLASSPATH} *.java
