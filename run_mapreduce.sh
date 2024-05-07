export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

hadoop com.sun.tools.javac.Main CalendarSubseter.java
jar cf calendar.jar CalendarSubseter*.class
hdfs dfs -rm -r -f /new-york/subset/calendar
hadoop jar calendar.jar CalendarSubseter /new-york/calendar.csv /new-york/subset/calendar
hdfs dfs -ls /new-york/subset/calendar
hadoop fs -cat /new-york/subset/calendar/* | head

hadoop com.sun.tools.javac.Main ListingsDetailedSubseter.java
jar cf listings_detailed.jar ListingsDetailedSubseter*.class
hdfs dfs -rm -r -f /new-york/subset/listings_detailed
hadoop jar listings_detailed.jar ListingsDetailedSubseter /new-york/listings_detailed.csv /new-york/subset/listings_detailed
hdfs dfs -ls /new-york/subset/listings_detailed
hadoop fs -cat /new-york/subset/listings_detailed/* | head

hadoop com.sun.tools.javac.Main ReviewsSubseter.java
jar cf reviews.jar ReviewsSubseter*.class
hdfs dfs -rm -r -f /new-york/subset/reviews
hadoop jar reviews.jar ReviewsSubseter /new-york/reviews_detailed.csv /new-york/subset/reviews
hdfs dfs -ls /new-york/subset/reviews
hadoop fs -cat /new-york/subset/reviews/* | head

hadoop com.sun.tools.javac.Main EventsSubseter.java
jar cf events.jar EventsSubseter*.class
hdfs dfs -rm -r -f /new-york/subset/events
hadoop jar events.jar EventsSubseter /new-york/NYC_events_filtered.csv /new-york/subset/events
hdfs dfs -ls /new-york/subset/events
hadoop fs -cat /new-york/subset/events/* | head

#ATTEMPT 2

hadoop com.sun.tools.javac.Main EventFilter.java
jar cf events.jar EventFilter*.class
hdfs dfs -rm -r -f /new-york/subset/events
hadoop jar events.jar EventFilter /new-york/NYC_events_filtered.csv /new-york/subset/events
hdfs dfs -ls /new-york/subset/events
hadoop fs -cat /new-york/subset/events/* | head

hadoop com.sun.tools.javac.Main EventCount.java
jar cf events.jar EventCount*.class
hdfs dfs -rm -r -f /new-york/subset/events_count
hadoop jar events.jar EventCount /new-york/subset/events/part-r-00000 /new-york/subset/events_count
hdfs dfs -ls /new-york/subset/events_count
hadoop fs -cat /new-york/subset/events_count/* | head

hadoop com.sun.tools.javac.Main Etap1.java
jar cf etap1.jar Etap1*.class
hdfs dfs -rm -r -f /new-york/subset/E1
hadoop jar etap1.jar Etap1 /new-york/calendar.csv /new-york/listings_detailed_cleaned.csv /new-york/subset/E1
hdfs dfs -ls /new-york/subset/E1
hadoop fs -cat /new-york/subset/E1/* | head

hadoop com.sun.tools.javac.Main Etap2.java
jar cf etap1.jar Etap2*.class
hdfs dfs -rm -r -f /new-york/subset/E2
hadoop jar etap1.jar Etap2 /new-york/subset/E1/part-r-00000 /new-york/subset/E2
hdfs dfs -ls /new-york/subset/E2
hadoop fs -cat /new-york/subset/E2/* | head

hadoop com.sun.tools.javac.Main Etap3.java
jar cf etap1.jar Etap3*.class
hdfs dfs -rm -r -f /new-york/subset/E3
hadoop jar etap1.jar Etap3 /new-york/subset/events_count/part-r-00000 /new-york/subset/E2/part-r-00000 /new-york/subset/E3
hdfs dfs -ls /new-york/subset/E3
hadoop fs -cat /new-york/subset/E3/* | head

hadoop com.sun.tools.javac.Main Etap3.java
jar cf etap1.jar Etap3*.class
hdfs dfs -rm -r -f /new-york/subset/E3
hadoop jar etap1.jar Etap3 /new-york/subset/events_count/part-r-00000 /new-york/subset/E2/part-r-00000 /new-york/subset/E3
hdfs dfs -ls /new-york/subset/E3
hadoop fs -cat /new-york/subset/E3/* | head

hadoop com.sun.tools.javac.Main Etap4.java
jar cf etap1.jar Etap4*.class
hdfs dfs -rm -r -f /new-york/subset/E4
hadoop jar etap1.jar Etap4 /new-york/subset/E3/part-r-00000 /new-york/subset/E4
hdfs dfs -ls /new-york/subset/E4
hadoop fs -cat /new-york/subset/E4/* | head

hadoop com.sun.tools.javac.Main Etap5.java
jar cf etap1.jar Etap5*.class
hdfs dfs -rm -r -f /new-york/subset/E5
hadoop jar etap1.jar Etap5 /new-york/weather_data.csv /new-york/subset/E4/part-r-00000 /new-york/subset/E5
hdfs dfs -ls /new-york/subset/E5
hadoop fs -cat /new-york/subset/E5/* | head