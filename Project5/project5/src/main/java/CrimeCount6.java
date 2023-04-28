package org.myorg;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CrimeCount6 extends Configured implements Tool {

    public static class CrimeCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        NullWritable nu = NullWritable.get();
        double dist = 350 * 3.28084;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] cols = line.split("\t");
            String crimeType = cols[4];
            if (crimeType.toLowerCase().equals("aggravated assault")) {
                double distance = Math.sqrt(Math.pow((1354326.897 - Double.valueOf(cols[0])), 2) + Math.pow((411447.7828 - Double.valueOf(cols[1])), 2));
                if (distance <= dist) {
                    word.set("oaklandaggravatedassaults");
                    context.write(word, one);
                }
            }
        }
    }

    public static class CrimeCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        NullWritable nu = NullWritable.get();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {

        Job job = new Job(getConf());
        job.setJarByClass(org.myorg.CrimeCount6.class);
        job.setJobName("crimecount6");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(org.myorg.CrimeCount6.CrimeCountMap.class);
        job.setCombinerClass(org.myorg.CrimeCount6.CrimeCountReducer.class);
        job.setReducerClass(org.myorg.CrimeCount6.CrimeCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        CrimeCountMap c = new CrimeCountMap();
        int result = ToolRunner.run(new org.myorg.CrimeCount6(), args);
        System.exit(result);
    }

}
