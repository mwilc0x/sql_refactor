#!/bin/bash

# There should be no reason to change this file
# If you think you have a valid reason, contact the TA first
#   Your code will be run with the original version of the file, 
#    which will not include any of your edits

JAVA_HOME=/usr/local/java1.6
PATH=$JAVA_HOME/bin:/bin:/usr/bin

if [ $# -ne 1 ]
then
	echo "Usage: $0 <env_file>"
	echo "  where env_file is an environment file containing settings"
	echo "       (like phase2_short.env or phase2.env)"
	exit 1
fi

source $1
 
MISSING_ENV_VAR=0
for e in RECS_DB_HOST RECS_DB_PORT RECS_DB_NAME RECS_DB_USER RECS_DB_PASSWORD \
         RECS_FILE HOLIDAY_FILE SPX_FILE OUT_FILE
do
  eval envVar=\$$e
  if [ -z "$envVar" ]; then
    echo "Requied environment variable $e is not set"
    MISSING_ENV_VAR=1
  fi
done
 
if [ $MISSING_ENV_VAR == 1 ]; then
  echo "Check your environment file $1"
  exit 1
fi 

MYSQL_EXEC="mysql -h$RECS_DB_HOST -P$RECS_DB_PORT -u$RECS_DB_USER \
                  -p$RECS_DB_PASSWORD --database=$RECS_DB_NAME"

echo 
echo "clearing database objects..."


$MYSQL_EXEC -BNe 'SELECT SPECIFIC_NAME \
                  FROM information_schema.ROUTINES \
                  WHERE ROUTINE_TYPE = "FUNCTION" \
                    AND ROUTINE_SCHEMA = DATABASE()' \
 | awk '{print "DROP FUNCTION IF EXISTS " $1 ";"}' \
 | $MYSQL_EXEC

if [ $? != 0 ]; then
  echo "ERROR: could not drop DB functions"
  echo "  Are your database credentials correct in $1?"
  exit 1
fi


$MYSQL_EXEC -BNe 'SELECT SPECIFIC_NAME \
                  FROM information_schema.ROUTINES \
                  WHERE ROUTINE_TYPE = "PROCEDURE" \
                    AND ROUTINE_SCHEMA = DATABASE()' \
 | awk '{print "DROP PROCEDURE IF EXISTS " $1 ";"}' \
 | $MYSQL_EXEC

if [ $? != 0 ]; then
  echo "ERROR: could not drop procecdures"
  exit 1
fi

$MYSQL_EXEC -BNe 'SELECT TABLE_NAME \
                  FROM information_schema.TABLES  \
                  WHERE TABLE_TYPE = "VIEW" \
                    AND TABLE_SCHEMA = DATABASE()' \
 | tr '\n' ',' | sed -e 's/,$//' \
 | awk '{print "SET FOREIGN_KEY_CHECKS = 0;\
                DROP VIEW IF EXISTS " $1 ";"}' \
 | $MYSQL_EXEC

if [ $? != 0 ]; then
  echo "ERROR: could not drop views"
  exit 1
fi

$MYSQL_EXEC -BNe 'SELECT TABLE_NAME \
                  FROM information_schema.TABLES  \
                  WHERE TABLE_TYPE = "BASE TABLE" \
                    AND TABLE_SCHEMA = DATABASE()' \
 | tr '\n' ',' | sed -e 's/,$//' \
 | awk '{print "SET FOREIGN_KEY_CHECKS = 0;\
                DROP TABLE IF EXISTS " $1 ";\
                SET FOREIGN_KEY_CHECKS = 1;"}' \
 | $MYSQL_EXEC

if [ $? != 0 ]; then
  echo "ERROR: could not drop tables"
  exit 1
fi


echo 
echo "running ant to compile code..."
ant
if [ $? != 0 ]; then
  exit 1
fi

echo 
echo "removing OUT_FILE ($OUT_FILE) ..."
rm -f $OUT_FILE


echo 
echo "running process..."
time java -Xmx8m -cp "classes:lib/mysql-connector-java-5.1.7-bin.jar" \
    edu.rutgers.cs336.EntryPoint

diff -q $OUT_FILE $VERIFY_FILE
if [ $? == 0 ]; then
  echo "SUCCESS: $OUT_FILE and $VERIFY_FILE match"
else
  echo "ERROR: $OUT_FILE and $VERIFY_FILE do not match"
  echo "  run \"diff $OUT_FILE $VERIFY_FILE\" for details"
fi
