package com.nvisia.bookclub.hadoop.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Writable class used to output the final results of the reduced ncaaf statistics.
 * 
 * @author Bryan Coller
 * 
 */
public class QuarterbackRatingWritable implements Writable {

	private DoubleWritable averageQBR;
	private Text teamName;
	
	public QuarterbackRatingWritable() {
		this.averageQBR = new DoubleWritable();
		this.teamName = new Text();
	}
	
	public QuarterbackRatingWritable(DoubleWritable averageQBR, Text teamName) {
		super();
		this.averageQBR = averageQBR;
		this.teamName = teamName;
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		averageQBR.readFields(dataInput);
		teamName.readFields(dataInput);		
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		averageQBR.write(dataOutput);
		teamName.write(dataOutput);
	}

	@Override
	public String toString() {
		return teamName + "\t" + averageQBR;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((averageQBR == null) ? 0 : averageQBR.hashCode());
		result = prime * result
				+ ((teamName == null) ? 0 : teamName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QuarterbackRatingWritable other = (QuarterbackRatingWritable) obj;
		if (averageQBR == null) {
			if (other.averageQBR != null)
				return false;
		} else if (!averageQBR.equals(other.averageQBR))
			return false;
		if (teamName == null) {
			if (other.teamName != null)
				return false;
		} else if (!teamName.equals(other.teamName))
			return false;
		return true;
	}
}
