#!/bin/sh
# build all other dependent jars in OTHER_JARS

JARS=`find /opt/bitnami/spark/jars/ -name '*.jar'`
OTHER_JARS=""
   for eachjarinlib in $JARS ; do    
if [ "$eachjarinlib" != "APPLICATIONJARTOBEADDEDSEPERATELY.JAR" ]; then
       OTHER_JARS=$eachjarinlib,$OTHER_JARS
fi
done
echo ---final list of jars are : $OTHER_JARS
echo $CLASSPATH

spark-submit --verbose --class com.main_package.AnomalyApp --jars $OTHER_JARS,APPLICATIONJARTOBEADDEDSEPERATELY.JAR app.jar localhost:9092 testinterface /home/