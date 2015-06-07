JAR_HOME=$CRN_HOME/sdk/java/lib

CLASSPATH=$JAVA_HOME/lib/tools.jar
for jar in axis axisReportNetClient commons-discovery commons-logging \
	standard dom4j jaxrpc saaj xalan xml-apis xercesImpl wsdl14j jstl; do
		CLASSPATH="$CLASSPATH:$JAR_HOME/$jar.jar"
done

CLASSPATH="$CLASSPATH:./:../Common:../Security:../ReportParams:../HandlerCS:../ReportSpec:../ViewCMReports:../ViewCMPackages:../ExecReports:.
./runreport:../Scheduler:../ContentStoReExplorer"

export CLASSPATH
