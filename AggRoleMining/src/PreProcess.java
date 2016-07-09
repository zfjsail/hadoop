import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created by hadoop on 16-7-7.
 */
public class PreProcess {
    public static class Pass1Mapper extends Mapper<Object, Text, Text, Text> {
        Set<String> words = new HashSet<>();
        public void setup(Context context) throws IOException {
            FileSystem hdfs = FileSystem.get(context.getConfiguration());
            String defaultPath = context.getConfiguration().get("fs.default.name");
            // System.out.println(defaultPath);
            FSDataInputStream peopleNameListName = hdfs.open(new Path(defaultPath + "/user/2016st28/People_List_unique.txt"));
            Scanner scanner = new Scanner(peopleNameListName);

            String line;
            while(scanner.hasNext()) {
                line = scanner.nextLine();
                words.add(line);
            }
            scanner.close();
            for(String i : words) {
                UserDefineLibrary.insertWord(i);
            }
            peopleNameListName.close();
        }
        public static int count = 0;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


            List<Term> terms = ToAnalysis.parse(value.toString()).getTerms();
            String result = new String();

            for(Term term : terms) {
                String termName = term.getName();
                if(words.contains(termName)) {
                    result += termName+"   ";
                }
            }

            context.write(new Text(String.valueOf(count++)) ,new Text(result));
        }
    }

    public static class Pass1Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         //   String value = values.iterator().next().toString();
          //  if(!value.isEmpty())
         //       context.write(null,new Text(value));
            for(Text val : values) {
                if(!val.toString().isEmpty()) {
                    context.write(null, val);
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = new Job(conf, "job1");
        job1.setJarByClass(PreProcess.class);
        job1.setMapperClass(PreProcess.Pass1Mapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(PreProcess.Pass1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);
    }
}