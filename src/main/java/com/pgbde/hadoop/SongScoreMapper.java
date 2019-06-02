package com.pgbde.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SongScoreMapper  extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {
	double dampingFactor = 0.05;	
	//xGe_ZX-U,df9a9dd86f161d059dc5c0e03a792617,1513252366,11,2017-12-14
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split("[,]");
		try{
//			String songId = split[0];
//			String userId = split[1];
//			String timestamp = split[2];
			Integer time = Integer.parseInt(split[3]);
//			String dateString = split[4]; 	
			double result = (1-dampingFactor)* (23-time);
			DoubleWritable score =  new DoubleWritable(result);
			
			context.write(new Text(split[0]+":"+split[4]), score);
		}catch(Exception e){
			System.out.print("SongScoreMappererror: "+value.toString());
		}
		
	}
}