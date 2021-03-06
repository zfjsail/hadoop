package InvertedIndex;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class NewPartitioner extends HashPartitioner<Text, IntWritable>{ 
	
	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		String term = new String();
		term = key.toString().split("#")[0];
		return super.getPartition(new Text(term), value, numReduceTasks);
	}

}
