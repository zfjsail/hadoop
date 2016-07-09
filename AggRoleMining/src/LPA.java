//import java.io.BufferedReader;
//import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LPA {
    public static class nMap extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> adjList = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            FileSystem hdfs = FileSystem.get(context.getConfiguration());
            String defaultPath = context.getConfiguration().get("fs.default.name");
            System.out.println("!"+defaultPath+"!");
            System.out.println(context.getConfiguration().get("input"));
            String filePath = new String(defaultPath+"/"+context.getConfiguration().get("input"));
            System.out.println(filePath);
            FSDataInputStream p = hdfs.open(new Path(filePath));
            Scanner scanner = new Scanner(p);


            String line;
            String[] tokens;

            while (scanner.hasNext()) {
                line = scanner.nextLine();
                tokens = line.split("\t");
                adjList.put(tokens[0], tokens[1]);
            }
           scanner.close();
            p.close();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String id = line.split("\t")[0];
            String[] list = line.split("\t")[1].split(",");
            Map<String, Integer> lableCount = new HashMap<>();
            ArrayList<String> lableList = new ArrayList<>();
            int max = 0;
            for (String item : list) {
                String lable = adjList.get(item);
                if (lableCount.containsKey(lable)) {
                    lableCount.put(lable, lableCount.get(lable) + 1);
                } else {
                    lableCount.put(lable, 1);
                }
                if (lableCount.get(lable) > max) {
                    max = lableCount.get(lable);
                }
            }
            for (String lable : lableCount.keySet()) {
                if (lableCount.get(lable) == max) {
                    lableList.add(lable);
                }
            }
            Random random = new Random(System.currentTimeMillis());
            context.write(new Text(id), new Text(lableList.get(random.nextInt(lableList.size()))));
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
            for (Text val : values) {
                mos.write("node", key, val);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    private static void deleteDir(Configuration conf, String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(dirPath);
        if (fs.exists(targetPath)) {
            boolean delResult = fs.delete(targetPath, true);
            if (delResult) {
                //System.out.println(targetPath + " has been deleted sucessfullly.");
            } else {
                //System.out.println(targetPath + " deletion failed.");
            }
        }

    }

    public static void main(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        for (int i = 0; i < 20; i++) {
            Configuration conf = new Configuration();
            conf.set("input", input + "/node-r-00000");
            Job job = new Job(conf, "lpa" + i);
            job.setJarByClass(LPA.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(nMap.class);
            job.setReducerClass(nReduce.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0] + "/edge-r-00000"));
            FileOutputFormat.setOutputPath(job, new Path(output + i));
            MultipleOutputs.addNamedOutput(job,"node",TextOutputFormat.class,Text.class,Text.class);
            input = output + i;
            job.waitForCompletion(true);
            if (i > 0 && i < 19) {
              //  deleteDir(conf, output + (i - 1));
            }
        }
    }
}