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
import java.util.List;

public class Etap1 {

    private static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable text, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split(",");
            Text listing_id = new Text(line[0]);
            String date = line[1];
            String price = line[3];
            context.write(listing_id, new Text("calendar\t" + date + "\t" + price));
        }
    }

    private static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable text, Text value, Context context) throws IOException, InterruptedException {

            String[] line = Utils.parseCSVLine(value.toString());
            Text listing_id = new Text(line[0]);
            String neighbourhood = line[29];
            String room_type = line[33];
            context.write(listing_id, new Text("listing\t" + neighbourhood + "\t" + room_type));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            List<String[]> linesCalendar = new ArrayList<>();
            String neighbourhood = "";
            String room_type = "";

            for (Text item : values) {
                String[] line = item.toString().split("\t");

                if ("calendar".equals(line[0])){
                    linesCalendar.add(line);
                }
                else {
                    neighbourhood = line[1];
                    room_type = line[2];
                }
            }

            for ( String[] line : linesCalendar){
                if (line.length == 3)
                    context.write(new Text(line[1]), new Text(neighbourhood + "\t" + room_type + "\t" + line[2]));
            }
        }

    }

    public static class Utils {
        private static final String CSV_REGEX = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

        public static String[] parseCSVLine(String line) {
            List<String> values = new ArrayList<>();

            String[] splitData = line.split(CSV_REGEX);

            for (String data : splitData) {
                if (data.startsWith("\"") && data.endsWith("\"")) {
                    data = data.substring(1, data.length() - 1);
                    data = data.replace("\"\"", "\"");
                }
                values.add(data);
            }

            return values.toArray(new String[0]);
        }

    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: Etap1 <path_to_calendar.csv> <path_to_listings.csv> <output_path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "etap1");
        job.setJarByClass(Etap1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

//        job.setCombinerClass(EventFilter.Reduce.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map2.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
