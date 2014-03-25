This project is used as a sandbox to test Hadoop functionality.

Specifically tested here is MapReduce.  The use-case is as follows:

- Download College Football historical data from: http://www.cfbstats.com/blog/college-football-data/
- Unzip each years stats data respectively.  The more team-game-statistics.csv files you have based on the year, the more data you can analyze.
- Move the latest years (2013) teams.csv to a folder called: "data/teams" within your Hadoop distribution
- Move the team-game-statistics.csv for each year into a folder called: "data/gamestats" within your Hadoop distribution.  Rename each team-game-statistics.csv file respectively for each year to avoid naming conflict.
- Execute code as such:
bin/hadoop jar <<PATH_TO_MAVEN_BUILT_JAR_FILE>>/hadoop-0.0.1-SNAPSHOT.jar com.nvisia.bookclub.hadoop.mapreduce.driver.NCAAFMapReduceDriver data/teams data/gamestats data/qbr-output
- Output will be every team in college football's quarterback rating (QBR) for all years of data given.  The formula for QBR is specified at: http://en.wikipedia.org/wiki/Passer_rating
