package InvertedIndex;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, IntWritable, FloatWritable, Text> {

	private Text word1 = new Text();
	private Text word2 = new Text();
	String temp = new String();
	static Text CurrentItem = new Text(" ");
	static List<String> postingList = new ArrayList<String>();
	static Map<Float, List<String>> rList = new TreeMap<Float, List<String>>().descendingMap();
	
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
			long count = 0;
			for(String p : postingList) {
				count += Long.parseLong(p.substring(p.indexOf(":") + 1));
			}
			float avg = count/((float)postingList.size());
			if(count > 0) {
				if(rList.containsKey(avg)) {
					rList.get(avg).add(CurrentItem.toString());
				}
				else {
					List<String> l = new ArrayList<String>();
					l.add(CurrentItem.toString());
					rList.put(avg, l);
				}
			}
			postingList = new ArrayList<String>();
		}
		CurrentItem = new Text(word1);
		postingList.add(word2.toString());
	}
	
	public void cleanup(Context context) 
					throws IOException, InterruptedException {
		long count = 0;
		for(String p : postingList) {
			count += Long.parseLong(p.substring(p.indexOf(":")+1));
		}
		float avg = count/((float)postingList.size());
		if(count > 0) {
			if(rList.containsKey(avg)) {
				rList.get(avg).add(CurrentItem.toString());
			}
			else {
				List<String> l = new ArrayList<String>();
				l.add(CurrentItem.toString());
				rList.put(avg, l);
			}
		}
		postingList = new ArrayList<String>();
		for(Entry<Float, List<String>> entry : rList.entrySet()) {
			  float key = entry.getKey();
			  List<String> value = entry.getValue();
			  
			 for(int i = 0; i < value.size(); i++) {
				 context.write(new FloatWritable(key), new Text(value.get(i)));
			 }
		}
		
	}

}