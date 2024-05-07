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

public class EventFilter {

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
//        private CSVParser parser;
//
//        @Override
//        protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
//            parser = new CSVParser(new StringReader(""), CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
//        }

        @Override
        public void map(LongWritable text, Text value, Context context) throws IOException, InterruptedException {

//            parser = parser.withReader(new StringReader(value.toString()));
            String[] line = value.toString().split(",");
            org.apache.hadoop.io.Text keyOut = new Text(line[0]);
            org.apache.hadoop.io.Text valueOut = new Text(line[2].split(" ")[0]+"\t"+line[6]);
            context.write(keyOut, valueOut);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Text t = values.iterator().next();
            String[] temp = t.toString().split("\t");
            context.write(new Text(temp[0]), new Text(temp[1]));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "events subset");
        job.setJarByClass(EventFilter.class);

        job.setMapperClass(EventFilter.Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

//        job.setCombinerClass(EventFilter.Reduce.class);

        job.setReducerClass(EventFilter.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
