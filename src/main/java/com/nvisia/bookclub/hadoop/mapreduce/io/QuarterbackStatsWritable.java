package com.nvisia.bookclub.hadoop.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * Writable domain class representing the game statistic data read in
 * specifically for a quarterback.
 * 
 * @author Bryan Coller
 */
public class QuarterbackStatsWritable implements Writable {

	private IntWritable attempts;
	private IntWritable completions;
	private IntWritable passingYards;
	private IntWritable touchdownPasses;
	private IntWritable interceptions;
	private LongWritable teamId;
	private LongWritable gameId;
	
	public QuarterbackStatsWritable() {
		this.attempts = new IntWritable();
		this.completions = new IntWritable();
		this.passingYards = new IntWritable();
		this.touchdownPasses = new IntWritable();
		this.interceptions = new IntWritable();
		this.teamId = new LongWritable();
		this.gameId = new LongWritable();
	}

	public QuarterbackStatsWritable(IntWritable attempts,
			IntWritable completions, IntWritable passingYards,
			IntWritable touchdownPasses, IntWritable interceptions,
			LongWritable teamId, LongWritable gameId) {
		this.attempts = attempts;
		this.completions = completions;
		this.passingYards = passingYards;
		this.touchdownPasses = touchdownPasses;
		this.interceptions = interceptions;
		this.teamId = teamId;
		this.gameId = gameId;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		attempts.readFields(dataInput);
		completions.readFields(dataInput);
		passingYards.readFields(dataInput);
		touchdownPasses.readFields(dataInput);
		interceptions.readFields(dataInput);
		teamId.readFields(dataInput);
		gameId.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		attempts.write(dataOutput);
		completions.write(dataOutput);
		passingYards.write(dataOutput);
		touchdownPasses.write(dataOutput);
		interceptions.write(dataOutput);
		teamId.write(dataOutput);
		gameId.write(dataOutput);
	}

	public IntWritable getAttempts() {
		return attempts;
	}

	public IntWritable getCompletions() {
		return completions;
	}

	public IntWritable getPassingYards() {
		return passingYards;
	}

	public IntWritable getTouchdownPasses() {
		return touchdownPasses;
	}

	public IntWritable getInterceptions() {
		return interceptions;
	}

	public LongWritable getTeamId() {
		return teamId;
	}

	public LongWritable getGameId() {
		return gameId;
	}
}
