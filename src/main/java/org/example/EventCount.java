//package org.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class EventCount {

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable text, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split("\t");
            org.apache.hadoop.io.Text keyOut = new Text(line[0]);
            org.apache.hadoop.io.Text valueOut = new Text(line[1]);
            context.write(keyOut, valueOut);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            List<String> nei = Arrays.asList("Bronx", "Manhattan", "Brooklyn", "Queens", "Staten Island");
            java.util.Map<String, Integer> map = new HashMap<>();
            for (Text value: values){
                if(nei.contains(value.toString()))
                    map.put(value.toString(), map.getOrDefault(value.toString(), 0) + 1);
            }
            for (java.util.Map.Entry<String, Integer> pair: map.entrySet()){
                context.write(key, new Text(pair.getKey() + "\t" + pair.getValue()));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "events subset");
        job.setJarByClass(EventCount.class);

        job.setMapperClass(EventCount.Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

//        job.setCombinerClass(EventFilter.Reduce.class);

        job.setReducerClass(EventCount.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
