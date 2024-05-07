//package org.hadoop;

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.time.DayOfWeek;
import java.time.LocalDate;

import java.io.IOException;
import java.text.DateFormat;
import java.util.*;

public class Etap4 {

    private static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable text, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String date = line[0];
            String neighbourhood = line [1];
            String room_type = line [2];
            String price = line[3];
            String events = line[4];
            if(!"price".equals(price))
                context.write(new Text(date), new Text(neighbourhood + "\t" + room_type + "\t" + price + "\t" + events));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            String[] holidayDates = {
                    "2023-03-12",
                    "2023-03-17",
                    "2023-04-09",
                    "2023-04-22",
                    "2023-05-05",
                    "2023-05-14",
                    "2023-05-29",
                    "2023-06-14",
                    "2023-06-18",
                    "2023-06-19",
                    "2023-07-04",
                    "2023-09-04",
                    "2023-09-11",
                    "2023-10-09",
                    "2023-10-31",
                    "2023-11-05",
                    "2023-11-11",
                    "2023-11-23",
                    "2023-12-25",
                    "2024-01-01",
                    "2024-01-15",
                    "2024-02-02",
                    "2024-02-14",
                    "2024-03-05"
            };

            String currDate = key.toString();
            String isHoliday = Arrays.stream(holidayDates).anyMatch(date -> date.equals(currDate)) ? "true" : "false";
            LocalDate date = LocalDate.parse(key.toString());
            DayOfWeek dayOfWeek = date.getDayOfWeek();
            String isWeekend = dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY ? "true" : "false";
            for (Text item : values) {
                String line = item.toString();
                context.write(key, new Text(line + "\t" + isWeekend + "\t" + isHoliday));
            }

        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Etap4 <output_path_etap3> <output_path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "etap4");
        job.setJarByClass(Etap4.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

//        job.setCombinerClass(EventFilter.Reduce.class);

        job.setReducerClass(Etap4.Reduce.class);
        job.setMapperClass(Etap4.Map1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
