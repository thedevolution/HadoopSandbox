package com.nvisia.bookclub.hadoop.mapreduce.mapper;

import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.nvisia.bookclub.hadoop.mapreduce.io.NestedWritable;
import com.nvisia.bookclub.hadoop.mapreduce.io.TeamWritable;

/**
 * Mapper class which pulls the CSV team data in and parses the relative
 * information to populate all {@link Team} attributes.
 * 
 * @author Bryan Coller
 * 
 */
public class TeamMapper extends Mapper<Object, Text, LongWritable, NestedWritable>{

	public void map(Object key, Text row, Context context)
			throws IOException, InterruptedException {
		final String[] rowValues = StringUtils.split(row.toString(), ',');
		if (!ArrayUtils.isEmpty(rowValues) && rowValues.length >= 3) {
			final Long teamId = NumberUtils.toLong(rowValues[0]);
			final TeamWritable teamWritable = new TeamWritable(new LongWritable(teamId), new Text(StringUtils.trimToNull(rowValues[1])));
			
			final NestedWritable nestedWritable = new NestedWritable();
			nestedWritable.setWritable(teamWritable);
			context.write(new LongWritable(teamId), nestedWritable);
		}
	}
}
