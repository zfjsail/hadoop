package InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumCombiner extends 
		Reducer<FloatWritable, Text, FloatWritable, Text>{
	
	@Override
	public void reduce(FloatWritable key, Iterable<Text>values, Context context)
					throws IOException, InterruptedException {
		String s = new String();
		for(Text val : values) {
			s += val.toString();
			s += " ";
		}
		context.write(key, new Text(s));
	}

}
