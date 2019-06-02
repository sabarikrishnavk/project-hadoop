package com.pgbde.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MusicSortJob extends Configured implements Tool {

	public String[] fetchArguments(String[] args) throws Exception {
//		args = new String[] { "input/input.txt", "output/test/"};
		if (args == null || args.length < 2) {
			String args0 = "args0: Input Directory of music stream \n";
			String args1 = "args1: Output Directory \n";
																									// ends
			throw new Exception(args0 + args1);
		} else {
			return args;
		}
	}

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();

		int returnStatus = ToolRunner.run(new Configuration(), new MusicSortJob(),
			args);

		long total = System.currentTimeMillis() - start;
		System.out.println("Computaion time taken: " + total / 60000 + " mins");
			
		System.exit(returnStatus);
	}

	public int run(String[] args) throws IOException {

		String[] arguments=args;
		try {
			arguments = fetchArguments(args);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		String input = arguments[0];
		String output =   arguments[1];
		

		Configuration conf= getConf();
		Job job =null;
		try {
			 
			
			//Second job to sort the songs per day.
			job= Job.getInstance(conf, "Song sort job");
			job.setJarByClass(MusicSortJob.class);

			job.setMapperClass(SongSortMapper.class);
				job.setMapOutputKeyClass(SongDayScoreKeyWritable.class);
				job.setMapOutputValueClass(NullWritable.class);
				
			job.setPartitionerClass(SongSortPartitioner.class);
			job.setSortComparatorClass(SongDayScoreSortComparator.class);
			job.setGroupingComparatorClass(SongDayScoreGroupingComparator.class);
			
			job.setReducerClass(SongSortReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(NullWritable.class);
				// Setting path to the input files
				FileInputFormat.addInputPath(job, new Path(input));
				// Setting path to the output files
				FileOutputFormat.setOutputPath(job, new Path(output));
			job.setNumReduceTasks(7);// each day in a week goes to a partition
			job.waitForCompletion(true);
		
			
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			 
		 return 0;
	}
}
