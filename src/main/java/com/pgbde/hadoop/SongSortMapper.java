package com.pgbde.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Mapper class to sort the songs based on date and score.
 * @author cloudera
 *
 */
public class SongSortMapper  extends
		Mapper<LongWritable, Text, SongDayScoreKeyWritable, NullWritable> {

//	Input : -1VwPtzs:2017-12-14	11.399999999999999
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, SongDayScoreKeyWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split("[\t]");
		try{
			String songId = split[0].split(":")[0];
			String dateString = split[0].split(":")[1];	 
			double score= Double.parseDouble(split[1]);  
			
			context.write(new SongDayScoreKeyWritable(songId,dateString,score), NullWritable.get());
		}catch(Exception e){
			System.out.print("SongSortMapper : error: "+value.toString());
		}
	}

}