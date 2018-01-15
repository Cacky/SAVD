package cn.edu.tjpu.SAVD;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapFifth extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString().trim(); // 将读取的值转换为字符串
		String[] num = line.split("\t");
		String[] num_1 = num[0].substring(1, num[0].length()-1).split(",");
		context.write(new Text(num_1[0]),value); // 将sax表示后的数据交给reduce
	}
}
