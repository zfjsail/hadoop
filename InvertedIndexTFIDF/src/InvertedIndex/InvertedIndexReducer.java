package InvertedIndex;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {

	private Text word1 = new Text();
	private Text word2 = new Text();
	String temp = new String();
	static Text CurrentItem = new Text(" ");
	static List<String> postingList = new ArrayList<String>();
	static Map<String, Integer> rfList = new TreeMap<String, Integer>();
	static Map<String, Integer> rnList = new TreeMap<String, Integer>();
	static Map<String, List<String>> author_map = new TreeMap<String, List<String>>();
	static List<Integer> occurList = new ArrayList<Integer>();

	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		temp = key.toString().split("~")[1]; //aname
		word1.set(temp + "~"+key.toString().split("~")[0]);
		String bname = key.toString().split("~")[2]; //bname
		for(IntWritable val : values) {
			sum += val.get();
		}
		word2.set(bname + ":" + sum);
		
		if(author_map.containsKey(temp)) {
			if(!author_map.get(temp).contains(bname))
				author_map.get(temp).add(bname);
		}
		else {
			List<String> l = new ArrayList<String>();
			l.add(bname);
			author_map.put(temp, l);
		}
		
		if(!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) {
			long count = 0;
			for(int p : occurList) {
				count += p;
			}
			if(count > 0) {
				rfList.put(CurrentItem.toString(), (int)count);
				rnList.put(CurrentItem.toString(), postingList.size());
			}
			postingList = new ArrayList<String>();
			occurList = new ArrayList<Integer>();
		}
		CurrentItem = new Text(word1);
		
		occurList.add(sum);
		if(!postingList.contains(bname))
			postingList.add(bname);
	}
	
	public void cleanup(Context context) 
					throws IOException, InterruptedException {
		long count = 0;
		for(int p : occurList) {
			count += p;
			
		}

		if(count > 0) {
			rfList.put(CurrentItem.toString(), (int)count);
			rnList.put(CurrentItem.toString(), postingList.size());
		}

		postingList = new ArrayList<String>();
		occurList = new ArrayList<Integer>();
		for(Entry<String, Integer> entry : rfList.entrySet()) {
			  String key = entry.getKey();
			  int tf = entry.getValue();
			  int absize = rnList.get(key);
			  
			  String author = key.split("~")[0];
			  String word = key.split("~")[1];
			  int abtotal = author_map.get(author).size();
			  double idf = Math.log(((double)abtotal) / (absize + 1));
			  
			 context.write(new Text(author + "\t" + word), new Text(tf + "\t" + idf));
		}
		
	}

}