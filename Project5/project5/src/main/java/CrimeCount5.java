package org.myorg;

import java.io.IOException;

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

public class CrimeCount5 extends Configured implements Tool {

    public static class CrimeCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        NullWritable nu = NullWritable.get();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] cols = line.split("\t");
            String crimeType = cols[4];
            if (crimeType.toLowerCase().equals("robbery") || crimeType.toLowerCase().equals("rape")) {
                word.set("rapeorrobbery");
                context.write(word, one);
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
        job.setJarByClass(org.myorg.CrimeCount5.class);
        job.setJobName("crimecount5");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(org.myorg.CrimeCount5.CrimeCountMap.class);
        job.setCombinerClass(org.myorg.CrimeCount5.CrimeCountReducer.class);
        job.setReducerClass(org.myorg.CrimeCount5.CrimeCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int result = ToolRunner.run(new org.myorg.CrimeCount5(), args);
        System.exit(result);
    }


}
