import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class Normalized {
    public static class nMap extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String keyPeople = line.split(",")[0];
            String valuePeople = line.split(",")[1];
            context.write(new Text(keyPeople), new Text(valuePeople));
        }
    }
    public static class nReduce extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs<Text, Text> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<>(context);
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            String out = "";
            String edge = "";
            Map<String, Double> vPeople = new HashMap<>();
            for (Text val : values) {
                String valuePeople = val.toString().split("\t")[0];
                Double countPeople = Double.parseDouble(val.toString().split("\t")[1]);
                vPeople.put(valuePeople, countPeople);
                sum += countPeople;
                mos.write("origEdge", key, new Text(valuePeople));
            }
            for (Map.Entry<String, Double> entry : vPeople.entrySet()) {
                out += entry.getKey() + ",";
                out += (entry.getValue() / sum) + ";";
                edge += entry.getKey() + ",";
            }
            context.write(key, new Text(out.substring(0, out.length() - 1)));
            mos.write("node", key, key);
            mos.write("edge", key, new Text(edge.substring(0, edge.length() - 1)));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "normalized");
        job.setJarByClass(Normalized.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(nMap.class);
        job.setReducerClass(nReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job,"node",TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"edge",TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"origEdge",TextOutputFormat.class,Text.class,Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}