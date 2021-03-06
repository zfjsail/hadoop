package InvertedIndex;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends
		Mapper<Object, Text, Text, IntWritable> {
	
	static Map<String, List<String>> author_map = new TreeMap<String, List<String>>();
	
	protected void setup(Context context) throws IOException {
		FileSplit fs = (FileSplit)context.getInputSplit();
		Path filepath = fs.getPath().getParent();
		FileSystem filesys = filepath.getFileSystem(context.getConfiguration());
		FileStatus[] fstatus = filesys.listStatus(filepath);
		for(int i = 0; i < fstatus.length; i++) {
			String name = fstatus[i].getPath().getName();
			String[] sp = name.split("([0-9]+)|(.txt)|(.TXT)");
			String aname = sp[0];
			String bname = sp[sp.length-2];
			if(author_map.containsKey(aname)) {
				if(!author_map.get(aname).contains(bname))
					author_map.get(aname).add(bname);
			}
			else {
				List<String> l = new ArrayList<String>();
				l.add(bname);
				author_map.put(aname, l);
			}
		}

	}
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String name = fileSplit.getPath().getName();
		String[] sp = name.split("([0-9]+)|(.txt)|(.TXT)");
		String aname = sp[0];
		String bname = sp[sp.length-2];
		int asum = author_map.get(aname).size();
		String temp = new String();
		String line = value.toString(); 
		StringTokenizer itr = new StringTokenizer(line);
		for(; itr.hasMoreTokens();) {
			temp = itr.nextToken();
			Text word = new Text();
			word.set(temp + "~" + aname + "~" + bname + "~" + asum);
			context.write(word, new IntWritable(1));
		}
	}
}