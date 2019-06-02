package com.pgbde.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SongDayScoreGroupingComparator extends  WritableComparator{

	public SongDayScoreGroupingComparator() {
		super(SongDayScoreKeyWritable.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		SongDayScoreKeyWritable key1 = (SongDayScoreKeyWritable) a;
		SongDayScoreKeyWritable key2 = (SongDayScoreKeyWritable) b;
		return key1.getDateString().compareTo(key2.getDateString());
	}
	
}