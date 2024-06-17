# cd /usr/local
# wget https://downloads.mariadb.com/Connectors/java/connector-java-2.7.4/mariadb-java-client-2.7.4.jar

spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f01_event_filtered.scala >> spark.log 2>&1
echo "f01 done"
spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f02_event_count.scala >> spark.log 2>&1
echo "f02 done"
spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f03_etap1.scala >> spark.log 2>&1
echo "f03 done"
spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f04_etap2.scala >> spark.log 2>&1
echo "f04 done"
spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f05_etap3.scala >> spark.log 2>&1
echo "f05 done"
spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f06_etap4.scala >> spark.log 2>&1
echo "f06 done"
spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f07_etap5.scala >> spark.log 2>&1
echo "f07 done"
