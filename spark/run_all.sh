# cd /usr/local
# wget https://downloads.mariadb.com/Connectors/java/connector-java-2.7.4/mariadb-java-client-2.7.4.jar

spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f01_event_filtered.scala >> journal.log 2>&1
status=$?
echo "f01_event_filtered.scala - exit status: $status, output file:" >> journal.log
echo "f01 done"

spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f02_event_count.scala >> journal.log 2>&1
status=$?
echo "f02_event_count.scala - exit status: $status, output file:" >> journal.log
echo "f02 done"

spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f03_etap1.scala >> journal.log 2>&1
status=$?
echo "f03_etap1.scala - exit status: $status, output file:" >> journal.log
echo "f03 done"

spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f04_etap2.scala >> journal.log 2>&1
status=$?
echo "f04_etap2.scala - exit status: $status, output file:" >> journal.log
echo "f04 done"

spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f05_etap3.scala >> journal.log 2>&1
status=$?
echo "f05_etap3.scala - exit status: $status, output file:" >> journal.log
echo "f05 done"

spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f06_etap4.scala >> journal.log 2>&1
status=$?
echo "f06_etap4.scala - exit status: $status, output file:" >> journal.log
echo "f06 done"

spark-shell --jars /usr/local/mariadb-java-client-2.7.4.jar -I f07_etap5.scala >> journal.log 2>&1
status=$?
echo "f07_etap5.scala - exit status: $status, output file:" >> journal.log
echo "f07 done"
