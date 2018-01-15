package cn.edu.tjpu.SAVD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapThird extends Mapper<LongWritable, Text, IntWritable, Text>{
	Text t = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString().trim(); // 将读取的值转换为字符串
		String[] num = line.split("\t");
		List<Double> l1 = new ArrayList<Double>();
		for (int i = 1; i < num.length; i++) {
			l1.add(Double.parseDouble(num[i]));
		}
		SAVDUtil.map2.put(Integer.parseInt(num[0]), l1);
	}
	
	protected void cleanup(Context context) throws IOException ,InterruptedException {
		for (Entry<String, List<String>> entry1 : SAVDUtil.map1.entrySet()) {
			List<String> l1 = new ArrayList<>();
			String[] ss = entry1.getValue().get(0).split(",");
			for (int i = 0,t = 0,count=0; i < ss.length; i++) {
				if (i < ss.length - 1) {
					if (t < SAVDUtil.VD_DP ) {
						l1.add(ss[i]);
						t++;
					}else {
						
						context.write(new IntWritable(count++), new Text(entry1.getKey()+"|"+l1.toString()));
						l1.clear();
						l1.add(ss[i]);
						t = 1;
					}					
				}else {	
					l1.add(ss[i]);
					context.write(new IntWritable(count++), new Text(entry1.getKey()+"|"+l1.toString()));
					l1.clear();
				}
				
			}

			

		}
		
		
		
	}
}
