package com.nvisia.bookclub.hadoop.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Writable domain class used for reading in the Team data for each given NCAAF
 * team.
 * 
 * @author Bryan Coller
 */
public class TeamWritable implements Writable {
	
	private LongWritable teamId;
	private Text name;
	
	public TeamWritable() {
		this.teamId = new LongWritable();
		this.name = new Text();
	}

	public TeamWritable(LongWritable teamId, Text name) {
		this.teamId = teamId;
		this.name = name;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		teamId.readFields(dataInput);
		name.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		teamId.write(dataOutput);
		name.write(dataOutput);
	}

	public LongWritable getTeamId() {
		return teamId;
	}

	public Text getName() {
		return name;
	}

}