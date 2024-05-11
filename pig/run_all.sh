pig f01_event_filtered.pig >> journal.log 2>&1
status=$?
echo "f01_event_filtered.pig - exit status: $status, output file:" >> journal.log
hadoop fs -cat /new-york/pig/events_filtered.csv/* | head >> journal.log

pig f02_event_count.pig >> journal.log 2>&1
status=$?
echo "f02_event_count.pig - exit status: $status, output file:" >> journal.log
hadoop fs -cat /new-york/pig/events_count.csv/* | head >> journal.log

pig f03_etap1.pig >> journal.log 2>&1
status=$?
echo "f03_etap1.pig - exit status: $status, output file:" >> journal.log
hadoop fs -cat /new-york/pig/etap1.csv/* | head >> journal.log

pig f04_etap2.pig >> journal.log 2>&1
status=$?
echo "f04_etap2.pig - exit status: $status, output file:" >> journal.log
hadoop fs -cat /new-york/pig/etap2.csv/* | head >> journal.log

pig f05_etap3.pig >> journal.log 2>&1
status=$?
echo "f05_etap3.pig - exit status: $status, output file:" >> journal.log
hadoop fs -cat /new-york/pig/etap3.csv/* | head >> journal.log

pig f06_etap4.pig >> journal.log 2>&1
status=$?
echo "f06_etap4.pig - exit status: $status, output file:" >> journal.log
hadoop fs -cat /new-york/pig/etap4.csv/* | head >> journal.log

pig f07_etap5.pig >> journal.log 2>&1
status=$?
echo "f07_etap5.pig - exit status: $status, output file:" >> journal.log
hadoop fs -cat /new-york/pig/etap5.csv/* | head >> journal.log

echo "summary: " >> journal.log
hadoop fs -cat /new-york/pig/events_filtered.csv/* | head >> journal.log
echo ""
hadoop fs -cat /new-york/pig/events_count.csv/* | head >> journal.log
echo ""
hadoop fs -cat /new-york/pig/etap1.csv/* | head >> journal.log
echo ""
hadoop fs -cat /new-york/pig/etap2.csv/* | head >> journal.log
echo ""
hadoop fs -cat /new-york/pig/etap3.csv/* | head >> journal.log
echo ""
hadoop fs -cat /new-york/pig/etap4.csv/* | head >> journal.log
echo ""
hadoop fs -cat /new-york/pig/etap5.csv/* | head >> journal.log
