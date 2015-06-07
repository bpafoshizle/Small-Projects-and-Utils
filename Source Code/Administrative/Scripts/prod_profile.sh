# this tells the software where oracle is installed
ORACLE_HOME=/db01ora9c/app/oracle/product/9i; export ORACLE_HOME
# this tells the software where java is installed
JAVA_HOME=/cognos/java5; export JAVA_HOME
# this tells the software what tns entry to use, i.e., oracle connection strings
TNS_ADMIN=/cognos/tns; export TNS_ADMIN
# this tells the software what odbc.ini entry to use, i.e., SQL Server connection strings
ODBCINI=/cognos/odbc/odbc.ini; export ODBCINI
# these are general paths that the software uses
PATH=$JAVA_HOME/bin:$ORACLE_HOME/bin:/cognos/performcentral/bin:/usr/local/bin:/usr/bin:/cognos/acrobat5/bin; export PATH
#  library paths that the software users.  I believe these are the main java file sets
LIBPATH=$ORACLE_HOME/lib32:$JAVA_HOME/lib:/cognos/odbc/lib:/cognos/performcentral/bin:$LIBPATH; export LIBPATH
#  had to do this to make the metric studio code work
NLS_LANG=AMERICAN_AMERICA.UTF8; export NLS_LANG
# this tells the trigger where the crn_home is located
CRN_HOME=/cognos/performcentral; export CRN_HOME
#  simply changing the path so you can run the cogbootstrapservice commands
cd /cognos/performcentral/bin
pwd

