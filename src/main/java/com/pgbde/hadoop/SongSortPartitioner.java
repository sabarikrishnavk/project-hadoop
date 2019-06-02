package com.pgbde.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
/**
 * Date based partition. 
 * Current partition is set as a mode of day which saves a days result into a file.
 * Easily map to trend per weekday. 
 * @author cloudera
 *
 */
public class SongSortPartitioner extends Partitioner<SongDayScoreKeyWritable,NullWritable>{

   
  public int getPartition(SongDayScoreKeyWritable key, NullWritable value, int numReduceTasks) {
	  try{
		  int date = Integer.parseInt( key.getDateString().split("-")[2]);
		  return (date % numReduceTasks);
	  }catch(Exception e){
		  System.out.println("SongSortPartitioner : "+ key);
		  return 0;
	  }
  }
}
