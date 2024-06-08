beeline -u 'jdbc:hive2://master:10000 hive org.apache.hive.jdbc.HiveDriver' -f /tmp/hive/f00_drops.sql >> journal.log 2>&1

hdfs dfs -rm -R /new-york/* >> journal.log 2>&1
hdfs dfs -put -f /tmp/calendar.csv /new-york/
hdfs dfs -put -f /tmp/listings_detailed_cleaned.csv /new-york/
hdfs dfs -put -f /tmp/NYC_events_filtered.csv /new-york/
hdfs dfs -put -f /tmp/reviews_detailed.csv /new-york/
hdfs dfs -put -f /tmp/weather_data.csv /new-york/
hdfs dfs -put -f /tmp/dates.csv /new-york/

beeline -u 'jdbc:hive2://master:10000 hive org.apache.hive.jdbc.HiveDriver' -f /tmp/hive/f01_event_filtered.sql >> journal.log 2>&1
status=$?
echo "f01_event_filtered.sql - exit status: $status" >> journal.log
hadoop fs -cat /user/hive/warehouse/nyc_data.db/nyc_events_filtered/* | head >> journal.log

beeline -u 'jdbc:hive2://master:10000 hive org.apache.hive.jdbc.HiveDriver' -f /tmp/hive/f02_event_count.sql >> journal.log 2>&1
status=$?
echo "f02_event_count.sql - exit status: $status" >> journal.log
hadoop fs -cat /user/hive/warehouse/nyc_data.db/events_summed/* | head >> journal.log

beeline -u 'jdbc:hive2://master:10000 hive org.apache.hive.jdbc.HiveDriver' -f /tmp/hive/f03_etap1.sql >> journal.log 2>&1
status=$?
echo "f03_etap1.sql - exit status: $status" >> journal.log
hadoop fs -cat /user/hive/warehouse/nyc_data.db/etap1_data/* | head >> journal.log

beeline -u 'jdbc:hive2://master:10000 hive org.apache.hive.jdbc.HiveDriver' -f /tmp/hive/f04_etap2.sql >> journal.log 2>&1
status=$?
echo "f04_etap2.sql - exit status: $status" >> journal.log
hadoop fs -cat /user/hive/warehouse/nyc_data.db/etap2_data/* | head >> journal.log

beeline -u 'jdbc:hive2://master:10000 hive org.apache.hive.jdbc.HiveDriver' -f /tmp/hive/f05_etap3.sql >> journal.log 2>&1
status=$?
echo "f05_etap3.sql - exit status: $status" >> journal.log
hadoop fs -cat /user/hive/warehouse/nyc_data.db/etap3_data/* | head >> journal.log

beeline -u 'jdbc:hive2://master:10000 hive org.apache.hive.jdbc.HiveDriver' -f /tmp/hive/f06_etap4.sql >> journal.log 2>&1
status=$?
echo "f06_etap4.sql - exit status: $status" >> journal.log
hadoop fs -cat /user/hive/warehouse/nyc_data.db/etap4_data/* | head >> journal.log

beeline -u 'jdbc:hive2://master:10000 hive org.apache.hive.jdbc.HiveDriver' -f /tmp/hive/f07_etap5.sql >> journal.log 2>&1
status=$?
echo "f07_etap5.sql - exit status: $status" >> journal.log
hadoop fs -cat /user/hive/warehouse/nyc_data.db/etap5_data/* | head >> journal.log
