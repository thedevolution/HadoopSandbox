package com.nvisia.bookclub.hadoop.mapreduce.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.nvisia.bookclub.hadoop.mapreduce.io.NestedWritable;
import com.nvisia.bookclub.hadoop.mapreduce.mapper.QuarterbackStatsMapper;
import com.nvisia.bookclub.hadoop.mapreduce.mapper.TeamMapper;
import com.nvisia.bookclub.hadoop.mapreduce.reducer.QuarterbackRatingReducer;

/**
 * Driver class used to fire-up a job against Hadoop. This process takes 3
 * inputs. They are:
 * <ul>
 * <li>Path of files containing Team data</li>
 * <li>Path of files containing Team Game Stats</li>
 * <li>Path for the output to be written to</li>
 * </ul>
 * During execution, this class deletes the output directory.
 * 
 * @author Bryan Coller
 */
public class NCAAFMapReduceDriver {

	public static void main(String[] args) throws IOException,
    	InterruptedException, ClassNotFoundException {
		final Path teamInputPath = new Path(args[0]);
		final Path gameStatsInputPath = new Path(args[1]);
		final Path outputDir = new Path(args[2]);
		
		final Configuration conf = new Configuration(true);
		
		final Job job = new Job(conf, "NCAAFMapReduce");
        job.setJarByClass(TeamMapper.class);
 
        // Setup MapReduce
        job.setReducerClass(QuarterbackRatingReducer.class);
        job.setNumReduceTasks(1);
 
        // The key class represents the TeamId
        job.setOutputKeyClass(LongWritable.class);
        
		// The output class is either the TeamWritable or the
		// QuarterbackStatsWritable. We will nest either of those in a
		// NestedWritable for the sake of specifying one class as the
		// OutputValueClass.
        job.setOutputValueClass(NestedWritable.class);
 
        // Multiple inputs will be populating different domain objects (Team or QuarterbackStats)
        MultipleInputs.addInputPath(job, teamInputPath, TextInputFormat.class, TeamMapper.class);
        MultipleInputs.addInputPath(job, gameStatsInputPath, TextInputFormat.class, QuarterbackStatsMapper.class);
 
        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir)) {
            hdfs.delete(outputDir, true);
        }
        
        // Execute job
        final int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
	}
}
