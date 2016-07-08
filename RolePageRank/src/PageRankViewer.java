import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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

public class PageRankViewer extends Configured implements Tool {
    public static class PageRankViewerMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
        private Text outPage = new Text();
        private FloatWritable outPr = new FloatWritable();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String page = line[0];
            float pr = Float.parseFloat(line[1]);
            outPage.set(page);
            outPr.set(pr);
            context.write(outPr, outPage);
        }
    }

    public static class PageRankViewerReducer extends Reducer<FloatWritable, Text, FloatWritable, Text> {
        public void reduce(FloatWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            for(Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static class DescendingFloatWritableComparable extends WritableComparator {
        public DescendingFloatWritableComparable() {
            super(FloatWritable.class, true);
        }

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }


    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("PageRankViewer");

        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setSortComparatorClass(DescendingFloatWritableComparable.class);

        job.setJarByClass(PageRankViewer.class);
        job.setMapperClass(PageRankViewerMapper.class);
        job.setReducerClass(PageRankViewerReducer.class);

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
        int res = ToolRunner.run(new Configuration(), new PageRankViewer(), args);
        //System.exit(res);
    }
}
