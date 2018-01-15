package cn.edu.tjpu.SAVD;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MapSecond extends Mapper<LongWritable, Text, Text, IntWritable>{
	private IntWritable v = new IntWritable();
	Text t = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString().trim(); // 将读取的值转换为字符串
		String[] num = line.split("\t");	
		double newNum,sum = 0, pp = 0;
		int k = 1; // k是聚合度累加器
		StringBuffer sax = new StringBuffer(); // new一个StringBuffer表示sax
		for (int i = 1; i < num.length; i++) {
			newNum =(Double.parseDouble(num[i]) - SAVDUtil.MIN) / (SAVDUtil.MAX - SAVDUtil.MIN);
			sum += newNum;
			if (k < SAVDUtil.DP) {
				if (i < num.length - 1) {
					k++;
				} else {
					pp = sum / k;
					sax.append(SAVDUtil.getSax(pp) + ","); // sax表示
				}
			} else {
				pp = sum / k;
				sax.append(SAVDUtil.getSax(pp) + ",");
				sum = 0;
				k = 1;
			}
		}	
		sax.deleteCharAt(sax.length() - 1);
		t.set(sax.toString());
		v.set(Integer.parseInt(num[0]));
		context.write(t,v);
		
	}
}
