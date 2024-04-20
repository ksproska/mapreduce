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

public class ReviewsSubseter { // listing_id,id,date,reviewer_id,reviewer_name,comments

    public static final int listing_id_inx = 0;
    public static final int id_inx = 1;
    public static final int comments_inx = 5;

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable Text, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            org.apache.hadoop.io.Text keyout = new Text(line[listing_id_inx].trim());
            String id = "";
            String comments = "";

            if (line.length > id_inx) {
                id = line[id_inx].trim();
            } else {
                System.out.println("Skipping line: " + value.toString());
            }
            if (line.length > comments_inx) {
                StringBuilder commentsBuilder = new StringBuilder();
                int inx = comments_inx;
                while (inx < line.length) {
                    commentsBuilder.append(line[inx]);
                    inx += 1;
                }
                comments = commentsBuilder.toString();
                comments = comments.replace("<br/>", "");
                comments = comments.replace("\"", "");
                comments = comments.replace("\n", "");
            } else {
                System.out.println("Skipping line: " + value.toString());
            }

            org.apache.hadoop.io.Text valueout = new Text(id + "," + comments);
            context.write(keyout, valueout);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "reviews subset");
        job.setJarByClass(ReviewsSubseter.class);
        job.setMapperClass(ReviewsSubseter.Map.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
