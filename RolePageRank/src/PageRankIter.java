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

public class PageRankIter extends Configured implements Tool{
    public static class PRIterMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String pageKey = line[0];
            double pr = Double.parseDouble(line[1]);
            String[] linkPages = line[2].split(";");
            for(String linkPage : linkPages) {
                String edge[] = linkPage.split(",");
                double weight = Double.parseDouble(edge[1]);
                String prValue = String.valueOf(pr * weight);
                context.write(new Text(edge[0]), new Text(prValue));
            }
            context.write(new Text(pageKey), new Text("|" + line[2]));
        }
    }

    public static class PRIterReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            String links = "";
            double pagerank = 0;
            for(Text val : values) {
                String tmp = val.toString();
                if(tmp.startsWith("|")) {
                    links = tmp.substring(tmp.indexOf("|") + 1);
                    continue;
                }
                pagerank += Double.parseDouble(val.toString());
            }
            pagerank = 0.15 + 0.85 * pagerank;
            context.write(key, new Text(String.valueOf(pagerank) + "\t" + links));
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("PageRankIter");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setJarByClass(PageRankIter.class);
        job.setMapperClass(PRIterMapper.class);
        job.setReducerClass(PRIterReducer.class);

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
        int res = ToolRunner.run(new Configuration(), new PageRankIter(), args);
        // System.exit(res);
    }
}
