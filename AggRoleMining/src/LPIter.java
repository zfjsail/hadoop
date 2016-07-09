import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import java.util.Enumeration;
import java.util.Hashtable;

public class LPIter extends Configured implements Tool {

    public static class LPIterMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String[] keyAndLabel = line[0].split(",");
            String Key = keyAndLabel[0];
            String[] edges = line[1].split(";");
            for(String edge : edges) {
                String neighbor= edge.split(",")[0];
                context.write(new Text(neighbor), new Text(line[0]));
            }
            context.write(new Text(Key), new Text("#" + keyAndLabel[1] + "#" + line[1]));
        }
    }

    public static class LPIterReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            String links = "";
            // String oldLabel = "";

            Hashtable<String, Double> hweight = new Hashtable<>();
            Hashtable<String, String> hlabel = new Hashtable<>();
            for(Text val : values) {
                String tmp = val.toString();
                if(tmp.startsWith("#")) {
                    String[] labelAndEdges = tmp.split("#"); // use "#"
                    // oldLabel = labelAndEdges[1];
                    links = labelAndEdges[2];
                    String[] edges = links.split(";");
                    for(String edge : edges) {
                        String[] info = edge.split(",");
                        double w = Double.parseDouble(info[1]);
                        hweight.put(info[0], w);
                    }
                }
                else {
                    String[] info = tmp.split(",");
                    hlabel.put(info[0], info[1]);
                }
            }

            Hashtable<String, Double> hlabelweight = new Hashtable<>();
            Enumeration<String> keys = hlabel.keys();
            while(keys.hasMoreElements()) {
                String vertex = keys.nextElement();
                String label = hlabel.get(vertex);
                double weight = hweight.get(vertex);
                if(hlabelweight.containsKey(label)) {
                    hlabelweight.put(label, hlabelweight.get(label) + weight);
                }
                else {
                    hlabelweight.put(label, weight);
                }
            }

            keys = hlabelweight.keys();
            double max = 0;
            String maxLabel = "";
            while(keys.hasMoreElements()) {
                String label = keys.nextElement();
                double w = hlabelweight.get(label);
                if(w > max) {
                    max = w;
                    maxLabel = label;
                }
            }

            context.write(new Text(key + "," + maxLabel), new Text(links));
        }
    }

    public int run(String[] args) throws Exception {
        Job job;

        job = Job.getInstance(getConf());
        job.setJobName(args[1]);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(LPIter.class);
        job.setMapperClass(LPIterMapper.class);
        job.setReducerClass(LPIterReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Path outputFilePath = new Path(args[1]);
        FileSystem fs =  FileSystem.newInstance(getConf());
        if(fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        int ret = job.waitForCompletion(true) ? 0 : 1;

        return ret;
    }

    public static void main(String[] args) throws Exception {
        int i = 0;
        Configuration conf = new Configuration();
        String[] inargs = {"", ""};
        int MAX_ITER_LOOP = 15; // tuned parameter -- nearly not update

        do {
            inargs[0] = args[0] + i;
            i++;
            inargs[1] = args[0] + i;

            ToolRunner.run(conf, new LPIter(), inargs);

        } while (i < MAX_ITER_LOOP);

        inargs[0] = args[0] + i;
        inargs[1] = args[0] + "Final";
        ToolRunner.run(new Configuration(), new LPIter(), inargs);

    }
}
