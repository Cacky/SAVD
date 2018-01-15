package cn.edu.tjpu.SAVD;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SAVD {

	public static void main(String[] args){
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		
		String firstInputPath = "D:/SparkIDEAWorkspace/database/inputSAVD";
		String firstOutputPath = "D:/SparkIDEAWorkspace/database/outputSAVD1";
		String secondOutputPath = "D:/SparkIDEAWorkspace/database/outputSAVD2";
		String thirdOutputPath = "D:/SparkIDEAWorkspace/database/outputSAVD3";
		String fourthOutputPath = "D:/SparkIDEAWorkspace/database/outputSAVD4";
		String fifthOutputPath = "D:/SparkIDEAWorkspace/database/outputSAVD5";
		
		try {
			FileSystem fs = FileSystem.get(conf);
			Job job1 = Job.getInstance(conf);
			job1.setJarByClass(SAVD.class);
			job1.setMapperClass(MapFirst.class);
			job1.setReducerClass(ReduceFirst.class);
			job1.setMapOutputKeyClass(Text.class); // map阶段的输出的key
			job1.setMapOutputValueClass(DoubleWritable.class); // map阶段的输出的value
			
			job1.setOutputKeyClass(Text.class); // reduce阶段的输出的key
			job1.setOutputValueClass(DoubleWritable.class); // reduce阶段的输出的value
			
			FileInputFormat.addInputPath(job1, new Path(firstInputPath));
			Path outputPath1 = new Path(firstOutputPath);
			if (fs.exists(outputPath1)) {
				fs.delete(outputPath1, true);
			}
			FileOutputFormat.setOutputPath(job1, outputPath1);
			
			Job job2 = Job.getInstance(conf);
			job2.setJarByClass(SAVD.class);

			job2.setMapperClass(MapSecond.class);
			job2.setReducerClass(ReduceSecond.class);

			job2.setMapOutputKeyClass(Text.class); // map阶段的输出的key
			job2.setMapOutputValueClass(IntWritable.class); // map阶段的输出的value

			job2.setOutputKeyClass(Text.class); // reduce阶段的输出的key
			job2.setOutputValueClass(DoubleWritable.class); // reduce阶段的输出的value

			// 输入路径是上一个作业的输出路径，因此这里填args[1],要和上面对应好
			FileInputFormat.addInputPath(job2, new Path(firstInputPath));
			Path outpath2 = new Path(secondOutputPath);
			if (fs.exists(outpath2)) {
				fs.delete(outpath2, true);
			}
			FileOutputFormat.setOutputPath(job2, outpath2);

			Job job3 = Job.getInstance(conf);
			job3.setJarByClass(SAVD.class);

			job3.setMapperClass(MapThird.class);
			job3.setReducerClass(ReduceThird.class);

			job3.setMapOutputKeyClass(IntWritable.class); // map阶段的输出的key
			job3.setMapOutputValueClass(Text.class); // map阶段的输出的value

			job3.setOutputKeyClass(Text.class); // reduce阶段的输出的key
			job3.setOutputValueClass(DoubleWritable.class); // reduce阶段的输出的value

			// 输入路径是上一个作业的输出路径，因此这里填args[1],要和上面对应好
			FileInputFormat.addInputPath(job3, new Path(firstInputPath));
			Path outpath3 = new Path(thirdOutputPath);
			if (fs.exists(outpath3)) {
				fs.delete(outpath3, true);
			}
			FileOutputFormat.setOutputPath(job3, outpath3);
			Job job4 = Job.getInstance(conf);
			job4.setJarByClass(SAVD.class);

			job4.setMapperClass(MapFourth.class);
			job4.setReducerClass(ReduceFourth.class);

			job4.setMapOutputKeyClass(Text.class); // map阶段的输出的key
			job4.setMapOutputValueClass(DoubleWritable.class); // map阶段的输出的value

			job4.setOutputKeyClass(Text.class); // reduce阶段的输出的key
			job4.setOutputValueClass(DoubleWritable.class); // reduce阶段的输出的value

			// 输入路径是上一个作业的输出路径，因此这里填args[1],要和上面对应好
			FileInputFormat.addInputPath(job4, new Path(thirdOutputPath));
			Path outpath4 = new Path(fourthOutputPath);
			if (fs.exists(outpath4)) {
				fs.delete(outpath4, true);
			}
			FileOutputFormat.setOutputPath(job4, outpath4);
			Job job5 = Job.getInstance(conf);
			job5.setJarByClass(SAVD.class);

			job5.setMapperClass(MapFifth.class);
			job5.setReducerClass(ReduceFifth.class);

			job5.setMapOutputKeyClass(Text.class); // map阶段的输出的key
			job5.setMapOutputValueClass(Text.class); // map阶段的输出的value

			job5.setOutputKeyClass(Text.class); // reduce阶段的输出的key
			job5.setOutputValueClass(DoubleWritable.class); // reduce阶段的输出的value

			// 输入路径是上一个作业的输出路径，因此这里填args[1],要和上面对应好
			FileInputFormat.addInputPath(job5, new Path(fourthOutputPath));
			Path outpath5 = new Path(fifthOutputPath);
			if (fs.exists(outpath5)) {
				fs.delete(outpath5, true);
			}
			FileOutputFormat.setOutputPath(job5, outpath5);
			if (job1.waitForCompletion(true)) {
				System.out.println("job1执行成功！！！");
				if (job2.waitForCompletion(true)) {
					System.out.println("job2执行成功！！！");
					if (job3.waitForCompletion(true)) {
						System.out.println("job3执行成功！！！");
						if (job4.waitForCompletion(true)) {
							System.out.println("job4执行成功！！！");
							if (job5.waitForCompletion(true)) {
								System.out.println("job5执行成功！！！");
							}
						}
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
