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

