package com.edureka.project.movie;

public class Utils {
	// movie data file
	public final static String FILE_MOVIE = "movies.csv";

	// data separator
	public final static String COMMA_SEP = ",";

	// parameter - count of what?
	public final static String COUNT_TYPE_KEY = "count.type";

	// A. Find the number of movies released between 1950 and 1960.
	// B. Find the number of movies having rating more than 4.
	// C. Find the number of movies with duration more than 2 hours (7200 second).
	// D. Find the total number of movies in the dataset.
	public final static String COUNT_A = "A";
	public final static String COUNT_B = "B";
	public final static String COUNT_C = "C";
	public final static String COUNT_D = "D";

	/**
	 * Column1: Movie ID 
	 * Column2: Movie name 
	 * Column3: Year of release 
	 * Column4: Rating of the movie 
	 * Column5: Movie duration in seconds
	 */
	public final static Integer MOVIE_ID = 0;// INT
	public final static Integer MOVIE_NAME = 1;// TEXT
	public final static Integer MOVIE_YEAR = 2;// INT
	public final static Integer MOVIE_RATING = 3;// DOUBLE
	public final static Integer MOVIE_DURATION = 4;// INT

}
