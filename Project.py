
import matplotlib
import pyspark.sql
import pandas as pd

# Create a SparkSession with Hive support
spark = (SparkSession.builder.appName("Movies").enableHiveSupport().getOrCreate())

# Define the HDFS path to the CSV file
hdfs_path = "hdfs://localhost:9000/user/hive/movies/movies.csv"
# Read the CSV file into a Spark DataFrame
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)
df.show()

# Check for missing values
from pyspark.sql.functions import isnan, when, count, col, explode, split
missing_values = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
missing_values.show()

# Drop duplicate rows
df = df.dropDuplicates()

# Define a dictionary with default values for each column to handle empty records
default_values = {
    'rating': 'Not Rated',
    'released': 'June 1',
    'score': '2.5',
    'votes': '1',
    'writer': 'blank',
    'star': 'blank',
    'country': 'Blank',
    'budget': '5000000',
    'gross': '5000000',
    'company': 'Blank',
    'runtime': '2'
}
# Replace empty strings with None
df = df.replace('', None)
# Use when to fill missing values
for column, default_value in default_values.items():
    df = df.withColumn(column, when(col(column).isNull(), default_value).otherwise(col(column)))
df.show(truncate=False)

# Check for null values in the DataFrame
for column in df.columns:
    null_count = df.filter(col(column).isNull()).count()
    print(f"Number of missing values in column '{column}': {null_count}")

# Save the DataFrame as a Hive table
table_name = "dataset"
df.write.saveAsTable(table_name, format="parquet", mode="overwrite")
#result
result=spark.sql("SELECT * FROM dataset")
result.show()

# number of movies per each year group by year
movies_in_each_year = spark.sql("""
    SELECT year, COUNT(*) as num_movies
    FROM {}
    GROUP BY year
    ORDER BY num_movies DESC
""".format(table_name))
movies_in_each_year.show(truncate=False)

# Query to get the count of movies in each genre
genre_count = spark.sql("""
    SELECT genre, COUNT(*) AS movie_count
    FROM {} 
    GROUP BY genre
""".format(table_name))
genre_count.show(truncate=False)

# Query to get the movie with the highest votes
top_votes_movie = spark.sql("""
    SELECT *
    FROM {} 
    ORDER BY votes DESC
    LIMIT 1
""".format(table_name))
top_votes_movie.show(truncate=False)


# Query to get the count of movies released over the years
movies_over_the_years = spark.sql("""
    SELECT year, COUNT(*) AS movie_count
    FROM {} 
    GROUP BY year
    ORDER BY year
""".format(table_name))
movies_over_the_years.show(truncate=False)

# Query the Hive table and get the writers with the most movies
writers_most_movies = spark.sql("""
    SELECT writer, COUNT(*) AS movie_count
    FROM {} 
    GROUP BY writer
    ORDER BY movie_count DESC
    LIMIT 1
""".format(table_name))
writers_most_movies.show(truncate=False)

# Query to get the stars with the most votes
stars_most_votes = spark.sql("""
    SELECT star, SUM(votes) AS total_votes
    FROM {} 
    GROUP BY star
    ORDER BY total_votes DESC
    LIMIT 1
""".format(table_name))


# Explore Patterns: Genre-wise Analysis
genre_analysis_df = df.groupBy("genre").agg({"score": "avg", "votes": "sum"}).orderBy("avg(score)", ascending=False)
genre_analysis_df.show()

high_budget_movies = df.filter(df["budget"] > 100000000)
df.printSchema()
df.describe().show()

# Group by country and calculate average budget and total gross earnings
import matplotlib.pyplot as plt
import seaborn as sns
# Group by country and calculate average budget and total gross earnings
budget_vs_gross_by_country_df = df.groupBy("country").agg({"budget": "avg", "gross": "sum"})

# Bar plot for average budget by country
plt.figure(figsize=(16, 8))  # Increase figsize
sns.barplot(x="avg(budget)", y="country", data=budget_vs_gross_by_country_df.toPandas())
plt.title("Average Budget by Country")
plt.xlabel("Average Budget")
plt.ylabel("Country")
plt.legend()
plt.show()

# Scatter plot for total gross earnings vs. average budget by country
plt.figure(figsize=(16, 8))  # Increase figsize
sns.scatterplot(x="avg(budget)", y="sum(gross)", hue="country", data=budget_vs_gross_by_country_df.toPandas())
plt.title("Total Gross Earnings vs. Average Budget by Country")
plt.xlabel("Average Budget")
plt.ylabel("Total Gross Earnings")
plt.legend()
plt.show()

# Group by genre and year, calculate average rating
genre_year_ratings_df = df.groupBy("genre", "year").agg({"score": "avg"}).orderBy("genre", "year")

# Convert PySpark DataFrame to Pandas for plotting
genre_year_ratings_pandas = genre_year_ratings_df.toPandas()

# Set the seaborn style
sns.set(style="whitegrid")

# Create a violin plot for the distribution of ratings within each genre over the years
plt.figure(figsize=(14, 8))
sns.violinplot(x="genre", y="avg(score)", data=genre_year_ratings_pandas, palette="husl", inner="quartile")

plt.title("Distribution of Ratings Within Each Genre Over the Years")
plt.xlabel("Genre")
plt.ylabel("Average Rating")
plt.show()

# Renaming the columns to make sure there are no spaces
genre_analysis_df = genre_analysis_df.withColumnRenamed("avg(score)", "avg_rating").withColumnRenamed("sum(votes)", "total_votes")
# Scatter Plot: Total Votes by Genre
plt.subplot(1, 2, 2)
sns.scatterplot(x="total_votes", y="genre", data=genre_analysis_df.toPandas())
plt.title("Total Votes by Genre")
plt.xlabel("Total Votes")
plt.ylabel("Genre")

plt.tight_layout()
plt.show()

# Group by director and calculate average rating and total number of movies
director_ratings_df = df.groupBy("director").agg({"score": "avg", "name": "count"}).orderBy("avg(score)", ascending=False)

# Set a different color palette
custom_palette = sns.color_palette("viridis", len(director_ratings_df.toPandas().head(10)))

# Bar plot for average rating by director
plt.figure(figsize=(12, 6))
sns.barplot(x="avg(score)", y="director", data=director_ratings_df.toPandas().head(10), palette=custom_palette)
plt.title("Average Ratings by Director (Top 10)")
plt.xlabel("Average Rating")
plt.ylabel("Director")
plt.show()

# Group by genre and calculate average budget and total gross earnings
budget_vs_gross_df = df.groupBy("genre").agg({"budget": "avg", "gross": "sum"})

# Bar plot for average budget by genre
plt.figure(figsize=(12, 6))
sns.barplot(x="avg(budget)", y="genre", data=budget_vs_gross_df.toPandas())
plt.title("Average Budget by Genre")
plt.xlabel("Average Budget")
plt.ylabel("Genre")

# Scatter plot for total gross earnings vs. average budget by genre
plt.figure(figsize=(12, 6))
sns.scatterplot(x="avg(budget)", y="sum(gross)", hue="genre", data=budget_vs_gross_df.toPandas())
plt.title("Total Gross Earnings vs. Average Budget by Genre")
plt.xlabel("Average Budget")
plt.ylabel("Total Gross Earnings")
plt.legend()
plt.show()

# Convert 'score' column to DoubleType to avoid serialization issues
df = df.withColumn("score", df["score"].cast(DoubleType()))

# Group by genre and year, calculate average score
genre_year_scores_df = df.groupBy("genre", "year").agg(F.avg("score").alias("avg_score")).orderBy("genre", "year")

# Plotting line charts for scores of movies in each genre over the years
plt.figure(figsize=(14, 8))
sns.set_palette("husl")  # Set color palette

# Collect distinct genres locally
distinct_genres = [row.genre for row in genre_year_scores_df.select("genre").distinct().collect()]

# Iterate through genres
for genre in distinct_genres:
    genre_data = genre_year_scores_df.filter(F.col("genre") == genre).toPandas()
    sns.lineplot(x="year", y="avg_score", data=genre_data, label=genre)

plt.title("Average Scores of Movies in Each Genre Over the Years")
plt.xlabel("Year")
plt.ylabel("Average Score")
plt.legend(title="Genre", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()

# Map on score, votes, budget and gross
numerical_columns = ['score', 'votes', 'budget', 'gross']
numerical_df = df.select(*numerical_columns)

# Calculate the correlation matrix
correlation_matrix = numerical_df.toPandas().corr()

# Plotting the heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f", linewidths=.5)
plt.title("Correlation Heatmap of Numerical Variables")
plt.show()

# Convert Spark DataFrame to Pandas DataFrame
pandas_df = df.toPandas()

# Group by genre and count the number of movies in each genre
genre_counts = pandas_df['Genre'].value_counts()

# Create a pie chart
plt.figure(figsize=(10, 6))
plt.pie(genre_counts, labels=genre_counts.index, autopct='%1.1f%%', startangle=140)
plt.title('Movie Genre Distribution (Top 10)')
plt.show()
spark.stop()
