# A small project on movie dataset

1. Find the number of movies released between 1950 and 1960.

2. Find the number of movies having rating more than 4.

3. Find the movies whose rating are between 3 and 4.

4. Find the number of movies with duration more than 2 hours (7200 second).

5. Find the list of years and number of movies released each year.

6. Find the total number of movies in the dataset.


Hadoop Command to execute:
  hadoop fs -put movies.csv
	hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_1950_1960_out A
	hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_high_rating_out B
	hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_count_bigger_out C
	hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_all_count_out D
