package com.pgbde.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class SongDayScoreKeyWritable implements Writable,WritableComparable<SongDayScoreKeyWritable> {
	private String songId;
	private String dateString;
	private double score;
	
	public SongDayScoreKeyWritable(){
		
	}

	public SongDayScoreKeyWritable(String songId, String dateString,
			double score) {
		super();
		this.songId = songId;
		this.dateString = dateString;
		this.score = score;
	}

	public void readFields(DataInput input) throws IOException {
		songId=WritableUtils.readString(input);
		dateString = WritableUtils.readString(input);
		score = Double.parseDouble(WritableUtils.readString(input));
	}

	public void write(DataOutput output) throws IOException {
		WritableUtils.writeString(output, songId);
		WritableUtils.writeString(output, dateString);
		WritableUtils.writeString(output, ""+score);
		
	}
	@Override
	public String toString() {
		return (new StringBuilder().append(songId).append("\t")
		.append(dateString).append("\t")
		.append(score)).toString();
	}

	public int compareTo(SongDayScoreKeyWritable obj2) {
		int result = dateString.compareTo(obj2.getDateString());
		
		
		if(result ==0){
			result = -1 * ((score >= obj2.getScore()) ? 1 : -1);
		}
		if(result ==0){
			result = -1 * songId.compareTo(obj2.songId);
		}		
		return 0;
	}

	public String getSongId() {
		return songId;
	}

	public void setSongId(String songId) {
		this.songId = songId;
	}

	public String getDateString() {
		return dateString;
	}

	public void setDateString(String dateString) {
		this.dateString = dateString;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}
	

}
