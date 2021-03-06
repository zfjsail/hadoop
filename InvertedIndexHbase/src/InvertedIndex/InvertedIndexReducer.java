package InvertedIndex;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.hbase.util.Bytes;

public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {

	private Text word1 = new Text();
	private Text word2 = new Text();
	String temp = new String();
	static Text CurrentItem = new Text(" ");
	static List<String> postingList = new ArrayList<String>();
	static Table table = null;
	static Connection connection = null;
	static Configuration conf = null;
	
	@Override
	public void setup(Context context) throws IOException {
		conf = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(conf);
		table = connection.getTable(TableName.valueOf("Wuxia"));
	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		word1.set(key.toString().split("~")[0]);
		temp = key.toString().split("~")[1];
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
			//byte[] bt = Bytes.toBytesBinary(CurrentItem.toString());
			Put row = new Put(Bytes.toBytes(CurrentItem.toString()));
			row.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("avg_times"), Bytes.toBytes(new Float(avg).toString()));
			if(count > 0) {
				context.write(CurrentItem, new Text(out.toString()));
				table.put(row);
			}
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
		//byte[] bt = Bytes.toBytesBinary(CurrentItem.toString());
		Put row = new Put(Bytes.toBytes(CurrentItem.toString()));
		row.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("avg_times"), Bytes.toBytes(new Float(avg).toString()));
		
		if(count > 0) {
			context.write(CurrentItem, new Text(out.toString()));
			table.put(row);
		}
		postingList = new ArrayList<String>();
		table.close();
		connection.close();
	}

}