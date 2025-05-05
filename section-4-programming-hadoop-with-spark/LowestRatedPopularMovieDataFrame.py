from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

# This function just creates a Python "dictionary" we can later
# use to convert movie ID's to movie names while printing out
# the final results.
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# Take each line of u.data and convert it to a Row (movieID, rating).
def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    # The main script - create our SparkSession.
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # Load up our movie ID -> movie name lookup table.
    movieNames = loadMovieNames()

    # Load up the raw u.data file.
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # Convert it to a RDD of Row objects with (movieID, rating)
    movies = lines.map(parseInput)

    # Convert it to a DataFrame.
    movieDataset = spark.createDataFrame(movies)

    # Compute average rating for each movieID.
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    # Compute count of ratings for each movieID.
    counts = movieDataset.groupBy("movieID").count()

    # Keep only movies with at least 10 ratings.
    moviesWithAtLeast10Ratings = counts.filter("count > 10")

    # Join the two together (we now have movieID, avg(rating), and count columns).
    averagesAndCounts = moviesWithAtLeast10Ratings.join(averageRatings, "movieID")

    # Pull the top 10 results.
    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)

    # Print them out, converting movie ID's to names as we go.
    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])


    # Stop the session.
    spark.stop()
