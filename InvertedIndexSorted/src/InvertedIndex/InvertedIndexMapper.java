package InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends
		Mapper<Object, Text, FloatWritable, Text> {
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString(); //.toLowerCase();
		float f = Float.parseFloat(line.substring(line.indexOf("\t")+1, line.indexOf(",")));
		String word = line.substring(0, line.indexOf("\t"));
		context.write(new FloatWritable(f), new Text(word));
		
	}

}