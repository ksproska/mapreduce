//package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CalendarSubseter {
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable Text, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            org.apache.hadoop.io.Text keyout = new Text(line[0]);
            org.apache.hadoop.io.Text valueout = new Text(line[1] + "," + line[3].replace("\"", "").replace("$", ""));
            context.write(keyout, valueout);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "calendar subset");
        job.setJarByClass(CalendarSubseter.class);
        job.setMapperClass(Map.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}