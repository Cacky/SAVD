package cn.edu.tjpu.SAVD;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceFirst extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	

	private double max_final = 0,min_final = 1000,tmp;
	private String s = new String();
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		s = key.toString();
		for (DoubleWritable a : values) {
			tmp = Double.parseDouble(a.toString());
			if (s.equals("MAX")) {
				if(tmp > max_final){
					max_final = tmp;
				}
			}else if (s.equals("MIN")) {
				if(tmp < min_final){
					min_final = tmp;
				}
			}
		}
		
		if (SAVDUtil.MAX < max_final) {
			SAVDUtil.MAX = max_final;
		}
		if (SAVDUtil.MIN > min_final) {
			SAVDUtil.MIN = min_final;
		}
		if(s.equals("MAX")){
			context.write(key, new DoubleWritable(max_final));		
		}else if (s.equals("MIN")) {
			context.write(key, new DoubleWritable(min_final));
		}
	}

}
