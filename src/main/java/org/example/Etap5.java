//package org.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Etap5 {
    private static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable text, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split(",");
            Text date = new Text(line[0]);
            String temperature = line[1];
            String condition = line[2];
            context.write(date, new Text(temperature + "\t" + condition));
        }
    }

    private static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable text, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split("\t");
            Text date = new Text(line[0]);
            String neighbourhood = line[1];
            String roomType = line[2];
            String price = line[3];
            String events = line[4];
            String isWeekend = line[5];
            String isHoliday = line[6];
            context.write(date, new Text(neighbourhood + "\t" + roomType + "\t" + price + "\t" + events + "\t" + isWeekend + "\t" + isHoliday));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            String temperature = "";
            String condition =  "";
            List<String> rows = new ArrayList<>();

            for(Text item : values){
                String[] line = item.toString().split("\t");
                if(line.length == 2) {
                    temperature = line[0];
                    condition = line[1];
                }
                else {
                    rows.add(item.toString());
                }
            }

            for(String item : rows){
                context.write(key, new Text(item + "\t" + temperature + "\t" + condition));
            }
        }

    }


    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: Etap5 <weather_data> <output_path_etap_4> <output_path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "etap5");
        job.setJarByClass(Etap5.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

//        job.setCombinerClass(EventFilter.Reduce.class);

        job.setReducerClass(Etap5.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Etap5.Map1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Etap5.Map2.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
