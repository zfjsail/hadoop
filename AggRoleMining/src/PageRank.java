/**
 * Created by hadoop on 16-7-4.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
public class PageRank {
    public static class Pass1Mapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr=new StringTokenizer(value.toString());
            String k = itr.nextToken();
            String v = itr.nextToken();
            //System.out.println("v: "+v);
            String[] lst = v.split(";");
            String val = new String();

            for(String i : lst) {
                //   System.out.println("item: "+i);
                val += i+" ";
            }

            // System.out.println(value+"\n"+v+"\n"+val);
            String initialPR = String.valueOf(1.0);
            context.write(new Text(k),new Text(initialPR+" "+val));
        }
    }

    public static class Pass1Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,values.iterator().next());
        }
    }

    public static class Pass2Mapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr=new StringTokenizer(value.toString());
            String k = itr.nextToken();
            double initial_pr = Double.parseDouble(itr.nextToken());
            String link_list = new String();

            while(itr.hasMoreTokens()) {
                link_list += itr.nextToken()+" ";
            }
            String[] lists = link_list.split("\\s");

            for(String term : lists) {
                String u = term.split(",")[0];

                double weight = Double.parseDouble(term.split(",")[1]);
                context.write(new Text(u),new Text(String.valueOf(initial_pr*weight)));
            }
            context.write(new Text(k),new Text(link_list));
        }
    }

    public static class Pass2Reducer extends Reducer<Text, Text, Text, Text> {
        static double d  = 0.8;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String link_list = "";
            double new_pr = 0.0;
            for(Text v : values) {
                String val = v.toString();
                try{
                    double vals = Double.parseDouble(val);
                    new_pr += d*vals;
                }
                catch (NumberFormatException e) {
                    link_list = v.toString();
                }
            }
            new_pr += (1-d);
            context.write(key,new Text(new_pr+" "+link_list));
        }
    }
    public static class Pass3Mapper extends Mapper<Object, Text, DoubleWritable, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr=new StringTokenizer(value.toString());
            String k = itr.nextToken();
            String v = itr.nextToken();
            context.write(new DoubleWritable(Double.parseDouble(v)),new Text(k));
        }
    }

    public static class Pass3Reducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text i : values)
                context.write(key,i);
        }
    }

    public static class MyComparator extends WritableComparator {
        public MyComparator() {
            super(DoubleWritable.class,true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
          //  System.out.println(a+" # "+b);
            DoubleWritable op1 = (DoubleWritable)a;
            DoubleWritable op2 = (DoubleWritable)b;
            return -1*op1.compareTo(op2);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();

        int times = 15;

        String prefix = "output/";

        String path1 = prefix + "data0/";

        Job job1 = new Job(conf, "job1");
        job1.setJarByClass(PageRank.class);
        job1.setMapperClass(Pass1Mapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(Pass1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(path1));
        job1.waitForCompletion(true);

        for(int i=0;i<times;i++) {
            Job job2 = null;
            job2 = new Job(conf, "job2");
            job2.setJarByClass(PageRank.class);
            job2.setMapperClass(Pass2Mapper.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setReducerClass(Pass2Reducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(prefix + "data"+String.valueOf(i)));
            FileOutputFormat.setOutputPath(job2, new Path(prefix + "data"+String.valueOf(i+1)));
            job2.waitForCompletion(true);
        }

        Job job3 = new Job(conf, "job3");
        job3.setJarByClass(PageRank.class);
        job3.setMapperClass(Pass3Mapper.class);
        job3.setMapOutputKeyClass(DoubleWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setReducerClass(Pass3Reducer.class);
        job3.setSortComparatorClass(MyComparator.class);
        job3.setOutputKeyClass(DoubleWritable.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(prefix + "data"+String.valueOf(times)));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        job3.waitForCompletion(true);

    }
}