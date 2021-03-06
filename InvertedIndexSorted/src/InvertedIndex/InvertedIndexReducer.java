package InvertedIndex;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<FloatWritable, Text, FloatWritable, Text> {

	String temp = new String();
	static Text CurrentItem = new Text(" ");
	static List<String> postingList = new ArrayList<String>();
	
	@Override
	public void reduce(FloatWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		String s = new String();
		for(Text val : values) {
			s += val.toString();
			s += "\n";
			s += key.toString();
			s += "\t";
		}
		s = s.substring(0, s.lastIndexOf("\n"));
		context.write(key, new Text(s));
	}
	

}