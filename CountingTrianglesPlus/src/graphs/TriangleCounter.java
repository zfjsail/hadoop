package graphs;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
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
import org.apache.hadoop.util.*;

public class TriangleCounter extends Configured implements Tool {

    // Directed -> Undirected
    public static class ToUndirectedMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        Text mKey = new Text();
        LongWritable mValue = new LongWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String e1,e2;
            if (tokenizer.hasMoreTokens()) {
                e1 = tokenizer.nextToken();
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("invalid edge line " + line);
                e2 = tokenizer.nextToken();
                // Input contains reciprocal edges, only need one.
                int r = e1.compareTo(e2);
                if (r < 0) {
                    mKey.set(e1 + "," + e2);
                    mValue.set(1);
                    context.write(mKey,mValue);
                }
                else if(r > 0) {
                    mKey.set(e2 + "," + e1);
                    mValue.set(-1);
                    context.write(mKey,mValue);
                }
            }
        }
    }

    public static class ToUndirectedReducer extends Reducer<Text, LongWritable, Text, Text>
    {
        Text mKey = new Text();
        Text mValue = new Text();

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException
        {
            boolean pos = false;
            boolean neg = false;
            Iterator<LongWritable> vs = values.iterator();
            String edge = key.toString();
            String first = edge.split(",")[0];
            String second = edge.split(",")[1];
            mKey.set(first);
            mValue.set(second);

            while (vs.hasNext()) {
                if(vs.next().get() == 1) pos = true;
                else neg = true;
                if(pos && neg) break;
            }
            if(pos && neg)
                context.write(mKey, mValue);
        }
    }


    // Maps values to Long,Long pairs.
    public static class ParseLongLongPairsMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        Text mKey = new Text();
        Text mValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String e1,e2;
            if (tokenizer.hasMoreTokens()) {
                e1 = tokenizer.nextToken();
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("invalid edge line " + line);
                e2 = tokenizer.nextToken();
                // Input contains reciprocal edges, only need one.

                mKey.set(e1);
                mValue.set(e2);
                context.write(mKey,mValue);
            }
        }
    }

    // Produces original edges and triads.
    public static class TriadsReducer extends Reducer<Text, Text, Text, LongWritable>
    {
        Text rKey = new Text();
        final static LongWritable zero = new LongWritable((byte)0);
        final static LongWritable one = new LongWritable((byte)1);
        String []vArray = new String[4096];
        int size = 0;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            // Produce triads - all permutations of pairs where e1 < e2 (value=1).
            // And all original edges (value=0).
            // Sorted by value.
            Iterator<Text> vs = values.iterator();
            for (size = 0; vs.hasNext(); ) {
                if (vArray.length==size) {
                    vArray = Arrays.copyOf(vArray, vArray.length*2);
                }

                String e = vs.next().toString();
                vArray[size++] = e;

                // Original edge.
                rKey.set(key.toString() + "," + e);
                context.write(rKey, zero);
            }

            Arrays.sort(vArray, 0, size);

            // Generate triads.
            for (int i=0; i<size; ++i) {
                for (int j=i+1; j<size; ++j) {
                    rKey.set(vArray[i] + "," + vArray[j]);
                    context.write(rKey, one);
                }
            }
        }
    }

    // Parses values into {Text,Long} pairs.
    public static class ParseTextLongPairsMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        Text mKey = new Text();
        LongWritable mValue = new LongWritable();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            if (tokenizer.hasMoreTokens()) {
                mKey.set(tokenizer.nextToken());
                if (!tokenizer.hasMoreTokens())
                    throw new RuntimeException("invalid intermediate line " + line);
                mValue.set(Long.parseLong(tokenizer.nextToken()));
                context.write(mKey, mValue);
            }
        }
    }

    // Counts the number of triangles.
    public static class CountTrianglesReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable>
    {
        long count = 0;
        final static LongWritable zero = new LongWritable(0);

        public void cleanup(Context context)
                throws IOException, InterruptedException
        {
            LongWritable v = new LongWritable(count);
            if (count > 0) context.write(zero, v);
        }

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException
        {
            long c = 0, n = 0;
            Iterator<LongWritable> vs = values.iterator();
            // Triad edge value=1, original edge value=0.
            while (vs.hasNext()) {
                c += vs.next().get();
                ++n;
            }
            if (c!=n) count += c;
        }
    }

    // Aggregates the counts.
    public static class AggregateCountsReducer extends Reducer<Text, LongWritable, LongWritable, LongWritable>
    {
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException
        {
            long sum = 0;
            Iterator<LongWritable> vs = values.iterator();
            while (vs.hasNext()) {
                sum += vs.next().get();
            }
            context.write(new LongWritable(sum), null);
        }
    }


    // Takes two arguments, the edges file and output file. Edges must be reciprocal, that is every
    // {source, dest} edge must have a corresponding {dest, source}.
    // File must be of the form:
    //   long <whitespace> long <newline>
    //   <repeat>
    public int run(String[] args) throws Exception
    {
        Job job1 = Job.getInstance(getConf());

        job1.setJobName("undirected");

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setJarByClass(TriangleCounter.class);
        job1.setMapperClass(ToUndirectedMapper.class);
        job1.setReducerClass(ToUndirectedReducer.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp1"));

        Job job2 = Job.getInstance(getConf());

        job2.setJobName("triads");

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setJarByClass(TriangleCounter.class);
        job2.setMapperClass(ParseLongLongPairsMapper.class);
        job2.setReducerClass(TriadsReducer.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path("temp1"));
        FileOutputFormat.setOutputPath(job2, new Path("temp2"));


        Job job3 = Job.getInstance(getConf());
        job3.setJobName("triangles");

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(LongWritable.class);

        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);

        job3.setJarByClass(TriangleCounter.class);
        job3.setMapperClass(ParseTextLongPairsMapper.class);
        job3.setReducerClass(CountTrianglesReducer.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job3, new Path("temp2"));
        FileOutputFormat.setOutputPath(job3, new Path("temp3"));


        Job job4 = Job.getInstance(getConf());
        job4.setJobName("count");

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(LongWritable.class);

        job4.setOutputKeyClass(LongWritable.class);
        job4.setOutputValueClass(LongWritable.class);

        job4.setJarByClass(TriangleCounter.class);
        job4.setMapperClass(ParseTextLongPairsMapper.class);
        job4.setReducerClass(AggregateCountsReducer.class);

        job4.setInputFormatClass(TextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job4, new Path("temp3"));
        FileOutputFormat.setOutputPath(job4, new Path(args[1]));

        Path outputFilePath = new Path("temp1");

		/* Delete output filepath if already exists */
        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        outputFilePath = new Path("temp2");

		/* Delete output filepath if already exists */

        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        outputFilePath = new Path("temp3");

		/* Delete output filepath if already exists */

        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        outputFilePath = new Path(args[1]);

		/* Delete output filepath if already exists */

        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }

        int ret = job1.waitForCompletion(true) ? 0 : 1;
        if (ret==0) ret = job2.waitForCompletion(true) ? 0 : 1;
        if (ret==0) ret = job3.waitForCompletion(true) ? 0 : 1;
        if (ret==0) ret = job4.waitForCompletion(true) ? 0 : 1;
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TriangleCounter(), args);
        System.exit(res);
    }

}
