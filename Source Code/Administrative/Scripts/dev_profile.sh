# this tells the software where oracle is installed
ORACLE_HOME=/db01ora9c/app/oracle/product/9i; export ORACLE_HOME
# this tells the software where java is installed
JAVA_HOME=/cognos/dev/java5; export JAVA_HOME
# this tells the software what tns entry to use, i.e., oracle connection strings
TNS_ADMIN=/cognos/dev/tns; export TNS_ADMIN
# this tells the software what odbc.ini entry to use, i.e., SQL Server connection strings
ODBCINI=/cognos/dev/odbc5.1/odbc.ini; export ODBCINI
#ODBCINI=/cognos/dev/odbc/odbc.ini; export ODBCINI
# these are general paths that the software uses
PATH=$JAVA_HOME/bin:.:$ORACLE_HOME/bin:~/bin:/usr/local/bin:/usr/bin:/usr/bin:/etc:/usr/sbin:/usr/ucb:/usr/bin/X11:/sbin:/usr/local/bin:/cognos/dev/performcentral/bin:/usr/local/bin:/usr/bin:/cognos/dev/acrobat5/bin:/usr/bin:/etc:/usr/sbin:/usr/ucb:/usr/bin/X11:/sbin:/usr/local/bin; export PATH
#  library paths that the software users.  I believe these are the main java file sets
LIBPATH=$ORACLE_HOME/lib32:$JAVA_HOME/lib:/cognos/dev/performcentral/bin:$LIBPATH:/cognos/dev/odbc5.1/lib; export LIBPATH
LIBPATH=$ORACLE_HOME/lib32:$JAVA_HOME/lib:/cognos/dev/performcentral/bin:/dazel/lib:/usr/lib:/db01ora9c/app/oracle/product/9i/lib32:/cognos/dev/odbc/lib; export LIBPATH
LIBPATH=/cognos/dev/odbc5.1/lib:$ORACLE_HOME/lib32:$JAVA_HOME/lib:/cognos/dev/performcentral/bin:/dazel/lib:/usr/lib; export LIBPATH
#  had to do this to make the metric studio code work
NLS_LANG=AMERICAN_AMERICA.UTF8; export NLS_LANG
#  simply changing the path so you can run the cogbootstrapservice commands
 alias ll='ls -lrt'
# this tells where the sdk is installed -schlagenhaf0
CRN_HOME=/cognos/dev/performcentral; export CRN_HOME

cd /cognos/dev/performcentral/bin
pwd
