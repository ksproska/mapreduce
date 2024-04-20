//package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class ListingsDetailedSubseter {
    public static final int id = 0;

    private static class Map extends Mapper<LongWritable, Object, Text, Text> {
        @Override
        public void map(LongWritable text, Object value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String neibourhood = "Other";

            if (line.contains(",Manhattan,")) neibourhood = "Manhattan";
            else if (line.contains(",Brooklyn,")) neibourhood = "Brooklyn";
            else if (line.contains(",Queens,")) neibourhood =  "Queens";
            else if (line.contains(",Bronx,")) neibourhood =  "Bronx";
            else if (line.contains(",Staten Island,")) neibourhood = "Staten Island";

            String roomType = "Other";
            if (line.contains(",Private room,")) roomType = "Private room";
            else if (line.contains(",Entire home/apt,")) roomType = "Entire home/apt";
            context.write(new Text(String.valueOf(id)), new Text(neibourhood + "," + roomType));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "listing subset");
        job.setJarByClass(ListingsDetailedSubseter.class);
        job.setMapperClass(Map.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}