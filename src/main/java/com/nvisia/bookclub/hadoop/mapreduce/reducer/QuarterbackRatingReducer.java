package com.nvisia.bookclub.hadoop.mapreduce.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.nvisia.bookclub.hadoop.mapreduce.io.NestedWritable;
import com.nvisia.bookclub.hadoop.mapreduce.io.QuarterbackRatingWritable;
import com.nvisia.bookclub.hadoop.mapreduce.io.QuarterbackStatsWritable;
import com.nvisia.bookclub.hadoop.mapreduce.io.TeamWritable;

/**
 * Reducer class used to pull the team data and the quarterback stats and reduce
 * them into a summary of the average quarterback rating for a given team over
 * the duration of all data presented.
 * 
 * @author Bryan Coller
 * 
 */
public class QuarterbackRatingReducer extends
		Reducer<LongWritable, NestedWritable, LongWritable, QuarterbackRatingWritable> {

	@Override
	public void reduce(LongWritable key, Iterable<NestedWritable> values, Context context)
			throws IOException, InterruptedException {
		
		final List<QuarterbackStatsWritable> quarterbackStatsWritables = new ArrayList<QuarterbackStatsWritable>();
		TeamWritable teamWritable = null;
		
		// Iterate over the nested writable instances and figure out what is what.
		for (NestedWritable nestedWritable : values) {
			if (nestedWritable.getWritable() instanceof TeamWritable) {
				// There "should" only be one team since our LongWritable is the
				// TeamId.
				teamWritable = (TeamWritable) nestedWritable.getWritable();
			} else if (nestedWritable.getWritable() instanceof QuarterbackStatsWritable) {
				quarterbackStatsWritables.add((QuarterbackStatsWritable) nestedWritable.getWritable());
			}
		}
		if (teamWritable != null) {
			// If we figured out what team we are dealing with, then we can move
			// on to that teams QB stats.
			int index = 0;
			// Initialize an array to place the passer ratings in that were gathered.
			final double[] passerRatings = new double[quarterbackStatsWritables.size()];
			for (QuarterbackStatsWritable writable : quarterbackStatsWritables) {
				passerRatings[index++] = calculatePasserRating(
						writable.getPassingYards().get(), 
						writable.getTouchdownPasses().get(), 
						writable.getCompletions().get(), 
						writable.getInterceptions().get(), 
						writable.getAttempts().get());
			}
			// Now that we have calculated all of the passer ratings, determine the average.
			double average = 0;
			for (int i = 0; i < passerRatings.length; i++) {
				average += passerRatings[i];
			}
			average = average / passerRatings.length;
			
			// If the average is of interest, then record it in the reduced output.
			if (average > 0) {
				final QuarterbackRatingWritable writable = new QuarterbackRatingWritable(
						new DoubleWritable(average), new Text(teamWritable.getName().toString()));
				context.write(key, writable);
			}
		}
	}
	
	/**
	 * Passer rating formula referenced from:
	 * http://en.wikipedia.org/wiki/Passer_rating
	 * 
	 * @param passYards
	 *            A quarterbacks passing yards for a given game
	 * @param touchdowns
	 *            A quarterbacks passing touchdowns for a given game
	 * @param completions
	 *            A quarterbacks passing completions for a given game
	 * @param interceptions
	 *            A quarterbacks passing interceptions for a given game
	 * @param attempts
	 *            A quarterbacks passing attempts for a given game
	 * @return Returns the calculated passer rating or zero if the quarterback
	 *         had no (zero) attempts
	 */
	private double calculatePasserRating(int passYards, int touchdowns, int completions, int interceptions, int attempts) {
		if (attempts != 0) {
			return ((8.4 * passYards) + (330 * touchdowns) + (100 * completions) - (200 * interceptions)) / attempts;
		}
		return 0;
	}
}
