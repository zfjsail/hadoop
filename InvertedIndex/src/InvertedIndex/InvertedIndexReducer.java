package InvertedIndex;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {

	private Text word1 = new Text();
	private Text word2 = new Text();
	String temp = new String();
	static Text CurrentItem = new Text(" ");
	static List<String> postingList = new ArrayList<String>();
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		word1.set(key.toString().split("#")[0]);
		temp = key.toString().split("#")[1];
		for(IntWritable val : values) {
			sum += val.get();
		}
		word2.set(temp + ":" + sum);
		if(!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) {
			StringBuilder out = new StringBuilder();
			long count = 0;
			for(String p : postingList) {
				out.append(p);
				out.append(";");
				count += Long.parseLong(p.substring(p.indexOf(":") + 1));
			}
			float avg = count/((float)postingList.size());
			if(count > 0)
				context.write(CurrentItem, new Text(avg + ","+out.toString()));
			postingList = new ArrayList<String>();
		}
		CurrentItem = new Text(word1);
		postingList.add(word2.toString());
	}
	
	public void cleanup(Context context) 
					throws IOException, InterruptedException {
		StringBuilder out = new StringBuilder();
		long count = 0;
		for(String p : postingList) {
			out.append(p);
			out.append(";");
			count += Long.parseLong(p.substring(p.indexOf(":")+1));
		}
		float avg = count/((float)postingList.size());
		if(count > 0)
			context.write(CurrentItem, new Text(avg + ","+out.toString()));
		postingList = new ArrayList<String>();
		
	}

}