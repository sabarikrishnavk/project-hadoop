package com.pgbde.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Counts the number of values associated with a key */

public class SongSortReducer extends Reducer<SongDayScoreKeyWritable, NullWritable,Text, NullWritable> {

	@Override
	public void reduce(SongDayScoreKeyWritable key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		for ( NullWritable value : values) {
			count++;
			if(count <=100){
				//System.out.println("sorted key : " +key +" count" +count);
				context.write(new Text(key.getSongId()+","+count+","+key.getDateString()),NullWritable.get());
			}
		}

	}
}
