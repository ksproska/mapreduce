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

public class Etap3 {

    private static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable text, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split("\t");
            Text date = new Text(line[0]);
            String neighbourhood = line[1];
            String amount = line[2];
            context.write(date, new Text(neighbourhood + "\t" + amount));
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
            context.write(date, new Text(neighbourhood + "\t" + roomType + "\t" + price));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            Map<String, List<String[]>> rows = new HashMap<>();
            Map<String, String> events = new HashMap<>();

            for (Text item : values) {
                String[] line = item.toString().split("\t");
                String neighborhood = line[0];
                if(line.length == 3){
                    List<String[]> roomType = rows.getOrDefault(neighborhood, new ArrayList<>());
                    roomType.add(line);
                    rows.put(neighborhood, roomType);
                }
                else {
                    events.put(neighborhood, line[1]);
                }
            }

            for (Map.Entry<String, List<String[]>> item: rows.entrySet()){
                String event = events.getOrDefault(item.getKey(), "0");
                String neighborhood = item.getKey();
                for (String[] rooms : item.getValue()) {
                    String roomType = rooms[1];
                    String price = rooms[2];
                    context.write(key, new Text(neighborhood + "\t" + roomType + "\t" + price + "\t" + event));
                }
            }
        }

    }


    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: Etap3 <output_path_event_count> <output_path_etap_2> <output_path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "etap3");
        job.setJarByClass(Etap3.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

//        job.setCombinerClass(EventFilter.Reduce.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Etap3.Map1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Etap3.Map2.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
