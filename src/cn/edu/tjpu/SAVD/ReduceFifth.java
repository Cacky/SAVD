package cn.edu.tjpu.SAVD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceFifth extends Reducer<Text, Text, Text, DoubleWritable>{
	

	private double distance = 0;
	String[] tmp,tmp1,tmp2 ;
	List<String> l1 = new ArrayList<String>();
	List<String> l2 = new ArrayList<String>();
	List<Double> ld1 = new ArrayList<Double>();
	List<Double> ld2 = new ArrayList<Double>();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for (Text a : values) {
			tmp = a.toString().split("\t");
			tmp1 = tmp[0].substring(1, tmp[0].length()-1).split(",");
			l1 = SAVDUtil.map1.get(tmp1[0]);
			l2 = SAVDUtil.map1.get(tmp1[1]);
			for (int i = 1; i < l1.size(); i++) {
				ld1 = SAVDUtil.map2.get(Integer.parseInt(l1.get(i)));
				for (int j = 1; j < l1.size(); j++) {
					if(i!= j){						
						ld2 = SAVDUtil.map2.get(Integer.parseInt(l1.get(j)));
						distance = SAVDUtil.histogram_intersection_distance(ld1,ld2);	
						context.write(new Text("<"+l1.get(i)+","+l1.get(j)+">"), new DoubleWritable(distance));			
					}
				}
				
				for (int s = 1; s < l2.size(); s++) {
					ld2 = SAVDUtil.map2.get(Integer.parseInt(l2.get(s)));
					distance = SAVDUtil.histogram_intersection_distance(ld1,ld2);
					context.write(new Text("<"+l1.get(i)+","+l2.get(s)+">"), new DoubleWritable(distance));
				}
			}
		}
	}

}
