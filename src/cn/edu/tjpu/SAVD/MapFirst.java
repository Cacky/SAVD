package cn.edu.tjpu.SAVD;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapFirst extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	private static final Text TEXT_MAX = new Text("MAX");
	private static final Text TEXT_MIN = new Text("MIN");
	private double max = 0,min =1000,tmp;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString().trim(); // 将读取的值转换为字符串
		String[] num = line.split("\t");		
		for (int i = 1; i < num.length; i++) {
			tmp = Double.parseDouble(num[i]);
			if(tmp > max) max = tmp;
			else if(tmp < min) min = tmp;			
		}
		context.write(TEXT_MAX,new DoubleWritable(max)); // 将sax表示后的数据交给reduce
		context.write(TEXT_MIN,new DoubleWritable(min)); // 将数据集中的最小值交给reduce
	}
}
