package InvertedIndex;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends
		Mapper<Object, Text, Text, IntWritable> {
	
	static Table table = null;
	static Connection connection = null;
	static Configuration conf = null;
	static Set<String> stop_words = new TreeSet<String>();
	
	@Override
	public void setup(Context context) throws IOException {
		conf = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(conf);
		table = connection.getTable(TableName.valueOf("stop_words"));
		
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for(Result r = scanner.next(); r != null; r = scanner.next()) {
			String str =Bytes.toString(CellUtil.cloneValue(r.listCells().get(0)));
			stop_words.add(str);
		}
		scanner.close();
		connection.close();
	}
	
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
			if(!stop_words.contains(temp)) {
				Text word = new Text();
				word.set(temp + "~" + fileName);
				context.write(word, new IntWritable(1));
			}
		}
	}
}