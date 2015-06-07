#!/bin/sh

#  Copyright 2006 Cognos Incorporated. All Rights Reserved.
#  Cognos and the Cognos logo are trademarks of Cognos Incorporated.

# CHANGE the following environment variables to point to Cognos 8 on your system.

if [ "$CRN_HOME" = "" ] ; then
	CRN_HOME=/usr/cognos/c8
fi

if [ "$JAVA_HOME" = "" ]; then
	JAVA_HOME="../../../bin/jre/1.4.2/"
fi

JAR_HOME="${CRN_HOME}/webapps/p2pd/WEB-INF/lib/"


CLASSPATH=".:${JAR_HOME}/tools.jar"
for jar in axis axisCrnpClient commons-discovery commons-logging jaxrpc \
        saaj xercesImpl xml-apis wsdl4j dom4j xalan ; do
  CLASSPATH="${CLASSPATH}:${CRN_HOME}/webapps/p2pd/WEB-INF/lib/${jar}.jar"
done
CLASSPATH="${CLASSPATH}:${CRN_HOME}/tomcat4.1.27/common/lib/servlet.jar"
CLASSPATH="${CLASSPATH}:${CRN_HOME}/tomcat4.1.27/server/lib/log4j-1.2.8.jar"

${JAVA_HOME}/bin/java -classpath ${CLASSPATH} Trigger ${1} ${2} ${3} ${4} ${5}
