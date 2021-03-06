package InvertedIndex;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends
		Mapper<Object, Text, Text, IntWritable> {
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		fileName = fileName.split("(.txt)|(.TXT)")[0];
		String temp = new String();
		String line = value.toString(); 
		StringTokenizer itr = new StringTokenizer(line);
		for(; itr.hasMoreTokens();) {
			temp = itr.nextToken();
			Text word = new Text();
			word.set(temp + "#" + fileName);
			context.write(word, new IntWritable(1));
		}
	}
}