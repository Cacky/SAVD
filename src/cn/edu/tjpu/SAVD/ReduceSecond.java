package cn.edu.tjpu.SAVD;



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceSecond extends Reducer<Text, IntWritable, Text, Text>{
	

	private List<String> old_pids;
	Text value ;
	//String[] splits;
	String sax;
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
		old_pids = new  ArrayList<String>();
		old_pids.add(key.toString());
		for (IntWritable a : values) {
			old_pids.add(a.toString());
		}
		//splits = key.toString().split(",");
		SAVDUtil.map1.put(UUID.randomUUID().toString().replace("-", ""), old_pids);
		context.write(new Text(UUID.randomUUID().toString().replace("-", "")),new Text(old_pids.toString()));
	}

}
