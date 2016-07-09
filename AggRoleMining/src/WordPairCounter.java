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
import java.util.Hashtable;


public class WordPairCounter extends Configured implements Tool{
    public static class WordPairMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] names = line.split(" +");

            for(int i = 0; i < names.length; i++) {
                for(int j = i + 1; j < names.length; j++) {
                    if(!names[i].equals(names[j])) {
                        context.write(new Text(names[i] + "," + names[j]), key);
                        context.write(new Text(names[j] + "," + names[i]), key);
                    }
                }
            }
        }
    }

    public static class WordPairCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            Hashtable<Long, Integer> ht = new Hashtable<>();
            long sum = 0;
            for(LongWritable val: values) {
                if(!ht.containsKey(val.get())) { // contains "key"
                    ht.put(val.get(), 1);
                    sum += 1;
                }
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class WordPairReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for(LongWritable val: values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("WordPairCounter");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setJarByClass(WordPairCounter.class);
        job.setMapperClass(WordPairMapper.class);
        job.setReducerClass(WordPairReducer.class);
        job.setCombinerClass(WordPairCombiner.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Path outputFilePath = new Path(args[1]);
        FileSystem fs = FileSystem.newInstance(getConf());
        if(fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        int ret = job.waitForCompletion(true) ? 0 : 1;
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordPairCounter(), args);
    }
}