# RankMovies
Finding top reviewed movies from csv data using Spark

Problem Statement:
	Problem-1: Rank the movie by their number of reviews(popularity) and select the top 10 highest ranked movies. 
	Problem-2: Find all the movies that its average rating is greater than 4 stars and also each of these movies  should have more than 10 reviews

Problem Solution:

1. RankMovies.java
	- it Contains implementation of both problems mentioned above
	- get_top_reviewed() method gives results for first problem
	- get_avg_rating() method gives results for second problem

2. pom.xml
	- it contains all the dependencies for above spark program to run

