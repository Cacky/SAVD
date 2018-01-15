package cn.edu.tjpu.SAVD;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceThird extends Reducer<IntWritable, Text, Text, DoubleWritable>{
	
	private double sax_distance;
	DoubleWritable dw = new DoubleWritable();
	Map<String, String[]> mapt = new HashMap<String, String[]>();

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text a : values) {
			String[] split = a.toString().trim().split("\\|");
			String[] sax_part = split[1].subSequence(1,split[1].length()-1).toString().split(",");
			mapt.put(split[0], sax_part);
		}
	
		for (Entry<String, String[]> entry1 : mapt.entrySet()) { // 对map1向量中的数据进行两两比较
			for (Entry<String, String[]> entry2 : mapt.entrySet()) {
				if (entry1 != entry2) {
					sax_distance = SAVDUtil.histogram_intersection_sax_distance(entry1.getValue(), entry2.getValue());
					dw.set(sax_distance);
					context.write(new Text("<"+entry1.getKey() + "," + entry2.getKey()+">"), dw);
				}
			}
		}		
	}
}
