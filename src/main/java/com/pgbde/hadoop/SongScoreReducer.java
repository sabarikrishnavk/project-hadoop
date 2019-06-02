package com.pgbde.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Counts the number of values associated with a key */

public class SongScoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		double count = 0;

		for (@SuppressWarnings("unused")
		DoubleWritable value : values) {
			count= count+ value.get();
		}
		context.write(key, new DoubleWritable(count));
	}
}
