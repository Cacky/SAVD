package cn.edu.tjpu.SAVD;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapFourth extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString().trim(); // 将读取的值转换为字符串
		String[] num = line.split("\t");
		Double distance = Double.parseDouble(num[1]);
		context.write(new Text(num[0]),new DoubleWritable(distance)); // 将sax表示后的数据交给reduce
	}
}
