@echo off

rem  Copyright 2006 Cognos Incorporated. All Rights Reserved.
rem  Cognos and the Cognos logo are trademarks of Cognos Incorporated.

rem Build Java files in directory EventTrigger

rem CHANGE the following environment variable to point to the Java Development Kit
rem on your system.

set JAVA_HOME=c:/jdk1.3.1_02
set CRN_HOME=../../../

set JAR_HOME=%CRN_HOME%sdk/java/lib
set JAVAC=%JAVA_HOME%/bin/javac

rem Create the Classpath

set CLASSPATH=
set CLASSPATH=%CLASSPATH%;%JAVA_HOME%/lib/tools.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/axis.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/axisCognosClient.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/commons-discovery.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/commons-logging.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/jaxrpc.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/saaj.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/xml-apis.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/xercesImpl.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/dom4j.jar
set CLASSPATH=%CLASSPATH%;%JAR_HOME%/xalan.jar

rem Compile Java files
"%JAVAC%" -classpath %CLASSPATH% *.java
