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
				
				StringBuilder output = new StringBuilder();
				output.append(key.getSongId())
					.append(",")
					.append(count)
					.append(",")
					.append(DateUtil.getNextDateString(key.getDateString()));
				//System.out.println("sorted key : " +key +" count" +count);
				context.write(new Text(output.toString()),NullWritable.get());
			}
		}

	}
}
