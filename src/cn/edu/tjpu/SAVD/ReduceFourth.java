package cn.edu.tjpu.SAVD;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceFourth extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	

	private double tmp=0,distance=0;
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		for (DoubleWritable a : values) {
			tmp = Double.parseDouble(a.toString());
			distance += tmp;
		}
		context.write(key, new DoubleWritable(distance));	
		distance = 0;
	}
}
