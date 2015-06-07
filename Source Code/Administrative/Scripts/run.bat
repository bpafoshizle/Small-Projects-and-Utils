@echo off

rem  Copyright 2006 Cognos Incorporated. All Rights Reserved.
rem  Cognos and the Cognos logo are trademarks of Cognos Incorporated.

rem Relative definitions based on Cognos 8 installation location.

set JAVA_HOME=../../../bin/jre/1.4.2/bin/java.exe
set CRN_HOME=../../../

set JAR_HOME=%CRN_HOME%sdk/java/lib

set CLASSPATH=.
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/axis.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/axisCognosClient.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/commons-discovery.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/commons-logging.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/jaxrpc.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/saaj.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/xml-apis.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/xercesImpl.jar

"%JAVA_HOME%" -classpath %CLASSPATH% Trigger %1 %2 %3 %4 %5
