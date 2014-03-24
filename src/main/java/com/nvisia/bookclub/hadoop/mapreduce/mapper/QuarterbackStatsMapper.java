package com.nvisia.bookclub.hadoop.mapreduce.mapper;

import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.nvisia.bookclub.hadoop.mapreduce.io.NestedWritable;
import com.nvisia.bookclub.hadoop.mapreduce.io.QuarterbackStatsWritable;

/**
 * A mapping class which pulls in the CSV game statistic information and pulls
 * the relative quarterback information for calculating the passer rating.
 * 
 * @author Bryan Coller
 * 
 */
public class QuarterbackStatsMapper extends Mapper<Object, Text, LongWritable, NestedWritable> {
	
	public void map(Object key, Text row, Context context) throws IOException,
			InterruptedException {
		final String[] rowValues = StringUtils.split(row.toString(), ',');
		if (!ArrayUtils.isEmpty(rowValues) && rowValues.length >= 60) {

			final Long teamId = NumberUtils.toLong(rowValues[0]);
			final Long gameId = NumberUtils.toLong(rowValues[1]);

			final QuarterbackStatsWritable quarterbackStatsWritable = new QuarterbackStatsWritable(
					new IntWritable(NumberUtils.toInt(rowValues[5])),
					new IntWritable(NumberUtils.toInt(rowValues[6])),
					new IntWritable(NumberUtils.toInt(rowValues[7])),
					new IntWritable(NumberUtils.toInt(rowValues[8])),
					new IntWritable(NumberUtils.toInt(rowValues[9])),
					new LongWritable(teamId),
					new LongWritable(gameId));

			final NestedWritable nestedWritable = new NestedWritable();
			nestedWritable.setWritable(quarterbackStatsWritable);
			context.write(new LongWritable(teamId), nestedWritable);
		}
	}
}
