package com.edureka.project.movie;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
A. Find the number of movies released between 1950 and 1960.
B. Find the number of movies having rating more than 4.
C. Find the number of movies with duration more than 2 hours (7200 second).
D. Find the total number of movies in the dataset.
E. Find the movies whose rating are between 3 and 4.
F. Find the list of years and number of movies released each year.
 */
public class MovieCount {

	// inner mapper class
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		protected void map(LongWritable key, Text value, Context context)
				throws java.io.IOException, InterruptedException {

			String row = value.toString();
			String[] tokens = row.split(Utils.COMMA_SEP);
			try {
				// considering movie name and year is mandatory field
				@SuppressWarnings("unused")
				String name = new String(tokens[Utils.MOVIE_NAME]);
				Integer year = Integer.parseInt(tokens[Utils.MOVIE_YEAR]);
				Double rating = 0.0d;
				// rating is not available in some field
				try {
					rating = Double.parseDouble(tokens[Utils.MOVIE_RATING]);
				} catch (Exception e) {
					rating = 0.0d;
				}

				Integer duration = 0;
				// if duration is empty, set to zero length
				try {
					duration = Integer.parseInt(tokens[Utils.MOVIE_DURATION]);
				} catch (Exception e) {
					duration = 0;
				}
				Configuration conf = context.getConfiguration();
				String counttype = conf.get(Utils.COUNT_TYPE_KEY).toUpperCase();
				if (counttype.equals(Utils.COUNT_A)) {
					// A. Find the number of movies released between 1950 and 1960.
					if ((year > 1950) && (year < 1960)) {
						context.write(new Text("1950-1960"), new LongWritable(1));
					}
				} else if (counttype.equals(Utils.COUNT_B)) {
					// B. Find the number of movies having rating more than 4.
					if (rating > 4.0) {
						context.write(new Text("rating>4"), new LongWritable(1));
					}
				} else if (counttype.equals(Utils.COUNT_C)) {
					// C. Find the number of movies with duration more than 2 hours (7200 second).
					if (duration > 7200) {
						context.write(new Text("duration>7200"), new LongWritable(1));
					}
				} else if (counttype.equals(Utils.COUNT_D)) {
					// D. Find the total number of movies in the dataset.
					context.write(new Text("total"), new LongWritable(1));
				} else if (counttype.equals(Utils.COUNT_E)) {
					// E. Find the movies whose rating are between 3 and 4.
					if ((rating > 3.0) && (rating < 4.0)) {
						context.write(new Text(name), new LongWritable(1));
					}
				} else if (counttype.equals(Utils.COUNT_F)) {
					// F. Find the list of years and number of movies released each year.
					context.write(new Text(year.toString()), new LongWritable(1));
				} else {
					// D. Find the total number of movies in the dataset.
					context.write(new Text("total"), new LongWritable(1));
				}
			} catch (Exception ee) {
				// ignore the invalid rows
				return;
			}
		}
	}

	// reducer to count
	public static class ReduceSum extends Reducer<Text, LongWritable, Text, LongWritable> {
		@SuppressWarnings("unused")
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			Long counter = 0L;
			for (LongWritable val : values) {
				++counter;
			}

			context.write(new Text(key), new LongWritable(counter));

		}
	}

	// hadoop fs -put movies.csv
	// hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_1950_1960_out A
	// hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_high_rating_out B
	// hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_count_bigger_out C
	// hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_all_count_out D
	// hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_list_avgrating_out E
	// hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_per_year_out F
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		if (args.length == 3) {
			conf.set(Utils.COUNT_TYPE_KEY, args[2]);
		} else {
			// default value
			conf.set(Utils.COUNT_TYPE_KEY, "D");
		}

		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf);
		job.setJarByClass(MovieCount.class);
		job.setJobName("Movie Count");
		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(ReduceSum.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
