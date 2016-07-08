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

public class GraphBuilder extends Configured implements Tool{

    public static class GraphBuilderMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String pagerank = "1.0";
            String[] line = value.toString().split("\t");
            context.write(new Text(line[0]), new Text(pagerank + "\t" + line[1]));
        }
    }

    public static class GraphBuilderReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            for(Text val: values) {
                context.write(key, val);
            }
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("GraphBuilder");

        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "\t");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setJarByClass(GraphBuilder.class);
        job.setMapperClass(GraphBuilderMapper.class);
        job.setReducerClass(GraphBuilderReducer.class);

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

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new GraphBuilder(), args);
        //System.exit(res);

    }
}
