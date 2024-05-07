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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Etap2 {

    private static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable text, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String date = line[0];
            String neighbourhood = line [1];
            String room_type = line [2];
            String price = line[3].replaceAll("[$,\"]","");
            if(!"price".equals(price))
                context.write(new Text(date), new Text(neighbourhood + "\t" + room_type + "\t" + price));
        }
    }

    public static class MapKey {
        public final String neighborhood;
        public final String roomType;

        public MapKey(String neighborhood, String roomType){
            this.neighborhood = neighborhood;
            this.roomType = roomType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MapKey)) return false;

            MapKey mapKey = (MapKey) o;

            if (!neighborhood.equals(mapKey.neighborhood)) return false;
            return roomType.equals(mapKey.roomType);
        }

        @Override
        public int hashCode() {
            int result = neighborhood.hashCode();
            result = 31 * result + roomType.hashCode();
            return result;
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            Map<MapKey, double[]> lines = new HashMap<>();

            for (Text item : values) {
                String[] line = item.toString().split("\t");
                String neighborhood = line[0];
                String roomType = line[1];
                double price = Double.parseDouble(line[2]);
                MapKey mapKey = new MapKey(neighborhood, roomType);
                double[] existingArray = lines.get(mapKey);

                if (existingArray == null) {
                    lines.put(mapKey, new double[] {price, 1});
                } else {
                    existingArray[0] += price;
                    existingArray[1]++;
                    lines.put(mapKey, existingArray);
                }
            }

            for (Map.Entry<MapKey, double[]> entry : lines.entrySet()) {
                Double average = entry.getValue()[0] / entry.getValue()[1];
                average = Precision.round(average, 2);
                context.write(key, new Text(entry.getKey().neighborhood + "\t" + entry.getKey().roomType + "\t" + average));
            }

        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Etap2 <output_path_etap1> <output_path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "etap1");
        job.setJarByClass(Etap2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

//        job.setCombinerClass(EventFilter.Reduce.class);

        job.setReducerClass(Reduce.class);
        job.setMapperClass(Map1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
