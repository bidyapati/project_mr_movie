# A small project on movie dataset

1. Find the number of movies released between 1950 and 1960.

    1950-1960,414
    
2. Find the number of movies having rating more than 4.

    rating>4,897
    
3. Find the movies whose rating are between 3 and 4.

    'Til Death,1
    
    'Til Death: Season 1,1
    
    'Til Death: Season 2,1
    
    'Til Death: Season 3,1
    
    'Til Death: Season 4,1
    
    'night  Mother,1
    
     10 Questions for the Dalai Lama,1
     
     10.5,2
     
     101 Dalmatians,1
     
     12 Dates of Christmas,1
     
     12 Signs of Love,2
     
     13 Going on 30,1
     
     1492: Conquest of Paradise,1
     
     16 to Life,1
     
     16-Love,1
     
     17 Girls,1
     
     18 to Life,1
     
     18 to Life: Season 1,1
     
     18 to Life: Season 2,1
     
     19 Kids and Counting,1
     
     19 Kids and Counting: Season 1,1
     
     ...
     
     ...
     
     ...

4. Find the number of movies with duration more than 2 hours (7200 second).

    duration>7200,641
    
5. Find the list of years and number of movies released each year.

    1913,3
    
    1914,20
    
    1915,1
    
    1916,1
    
    1918,1
    
    1919,3
    
    1920,6
    
    1921,2
    
    1922,2
    
    1923,4
    
    1924,5
    
    1925,5
    
    1926,2
    
    ...
    
    ...
    
    ...
    
    
6. Find the total number of movies in the dataset.

    total,49590

Hadoop Command to execute:

        
	hadoop fs -put movies.csv
	hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_1950_1960_out A
	hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_high_rating_out B
	hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_count_bigger_out C
	hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_all_count_out D
	hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_list_avgrating_out E
	hadoop jar /mnt/home/edureka_1270998/jars/edurekaProjects.jar com.edureka.project.movie.MovieCount movies.csv movies_per_year_out F
	
