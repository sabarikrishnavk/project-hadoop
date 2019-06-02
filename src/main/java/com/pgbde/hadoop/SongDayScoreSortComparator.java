package com.pgbde.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SongDayScoreSortComparator extends  WritableComparator{

	public SongDayScoreSortComparator() {
		super(SongDayScoreKeyWritable.class,true);
	}
	
	//Sort by date and the score;

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		SongDayScoreKeyWritable key1 = (SongDayScoreKeyWritable) a;
		SongDayScoreKeyWritable key2 = (SongDayScoreKeyWritable) b;
		int result= key1.getDateString().compareTo(key2.getDateString());
		
		if(result == 0){
			result = -1 *((key1.getScore() >= key2.getScore()) ? 1 : -1);
		}
		if(result == 0){
			result = -1 * key1.getSongId().compareTo(key2.getSongId());
		}
		return result;
	}
	
}