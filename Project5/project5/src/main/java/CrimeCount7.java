package org.myorg;
import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CrimeCount7 extends Configured implements Tool {

    public static class CrimeCountMap extends Mapper<LongWritable, Text, Text, Text>
    {

        private Text word = new Text();
        private Text val = new Text();

        double dist = 300 * 3.28084;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

            String line = value.toString();

            String[] cols = line.split("\t");
            String crimeType = cols[4];
            if (crimeType.toLowerCase().equals("aggravated assault")) {
                double distance = Math.sqrt(Math.pow((1354326.897 - Double.valueOf(cols[0])), 2) + Math.pow((411447.7828 - Double.valueOf(cols[1])), 2));
                if (distance <= dist) {
                    val.set(String.format("%s,%s", cols[8], cols[7]));
                    word.set("crime");
                    context.write(word, val);
                }
            }
        }
    }

    public static class CrimeCountReducer extends Reducer<Text, Text, Text, Text>
    {
        private StringBuilder out;
        private Text val = new Text();
        private Text outkey = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {

            String header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<kml xmlns=\"http://www.opengis.net/kml/2.2\" xmlns:gx=\"http://www.google.com/kml/ext/2.2\" xmlns:kml=\"http://www.opengis.net/kml/2.2\" xmlns:atom=\"http://www.w3.org/2005/Atom\">\n" +
                    "<Document>\n" +
                    "        <name>oakland assualt.kml</name>\n" +
                    "        <Style id=\"sn_ylw-pushpin\">\n" +
                    "                <PolyStyle>\n" +
                    "                        <outline>0</outline>\n" +
                    "                </PolyStyle>\n" +
                    "        </Style>\n" +
                    "        <Placemark>\n" +
                    "                <name>aggregate assualt</name>\n" +
                    "                <styleUrl>#sn_ylw-pushpin</styleUrl>\n" +
                    "                <Polygon>\n" +
                    "                        <tessellate>1</tessellate>\n" +
                    "                        <altitudeMode>relativeToGround</altitudeMode>\n" +
                    "                        <outerBoundaryIs>\n" +
                    "                                <LinearRing>\n" +
                    "                                        <coordinates>";

            out = new StringBuilder();

            for (Text value: values) {
                out.append(value.toString() + " ");
            }

            String footer = "</coordinates>\n" +
                    "                                </LinearRing>\n" +
                    "                        </outerBoundaryIs>\n" +
                    "                </Polygon>\n" +
                    "        </Placemark>\n" +
                    "</Document>\n" +
                    "</kml>\n";

            val.set(header + out.toString() + footer);
            outkey.set("");
            context.write(outkey, val);
        }
    }

    public int run(String[] args) throws Exception  {

        Job job = new Job(getConf());
        job.setJarByClass(org.myorg.CrimeCount7.class);
        job.setJobName("crimecount7");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(org.myorg.CrimeCount7.CrimeCountMap.class);
        job.setReducerClass(org.myorg.CrimeCount7.CrimeCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0: 1;
    }


    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int result = ToolRunner.run(new org.myorg.CrimeCount7(), args);
        System.exit(result);
    }


}
