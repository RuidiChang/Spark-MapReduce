import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class oaklandcrimestatskml extends Configured implements Tool {

    public static class CrimeCountMap extends Mapper<LongWritable, Text, NullWritable, Text> {
        private NullWritable word;
        private Text location = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double dist = 350 * 3.28084;
            String line = value.toString();
            String[] cols = line.split("\t");
            String crimeType = cols[4];
            if (crimeType.equalsIgnoreCase("aggravated assault")) {
                double distance = Math.sqrt(Math.pow((1354326.897 - Double.valueOf(cols[0])), 2) + Math.pow((411447.7828 - Double.valueOf(cols[1])), 2));
                if (distance <= dist) {
                    location.set(String.format("%s,%s", cols[8], cols[7]));
                    context.write(word, location);
                }
            }
        }
    }

    public static class CrimeCountReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        private Text location = new Text();

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n" +
                    "<Document>\n" +
                    "<name>Document.kml</name>\n" +
                    "<open>1</open>\n";
            String footer = "</Document>\n" + "</kml>";
            String place = "";
            for (Text value : values) {
                place += "<Placemark>\n" + "<Point>\n" + "<coordinates>" + value + "</coordinates>" + "</Point>\n" + "</Placemark>\n";
            }
            location.set(header + place + footer);
            context.write(key, location);
        }
    }

    public int run(String[] args) throws Exception {

        Job job = new Job(getConf());
        job.setJarByClass(oaklandcrimestatskml.class);
        job.setJobName("oaklandcrimestatskml");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(oaklandcrimestatskml.CrimeCountMap.class);
        job.setReducerClass(oaklandcrimestatskml.CrimeCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int result = ToolRunner.run(new oaklandcrimestatskml(), args);
        System.exit(result);
    }


}
