# Data-Analysis-on-Movies-dataset-using-Hadoop-Hive-and-PySpark
This is my first ever project related to data analysis and I am proud of it. I sat day and night for four days to set up the environment and integrate Hadoop, hive and spark. This has been hard for me as I have no experience in data analysis and had no knowledge of PySpark before this. This project made me believe in myself once again. This is a small project considering the perspective of a technical expert but this project that I did in my first semester, for the fall of 2023 is what gave me confidence that I can do anything if I set my mind to it. 
The source code included the original code that I used for the project. I set the environment on my local machine; detailed documentation on the same can be found in the presentation file. 
Many times, I wished for relaxation regarding where we could get the data file. I might have used Amazon S3 bucket as I felt that was a way easier process. However, the entire project journey only made me believe in myself, so I have no regrets.

This project analyzes a movie dataset using PySpark for data processing, Hive for SQL queries, and Matplotlib and Seaborn for data visualization. The objective is to extract meaningful insights about movies, genres, budgets, and other key metrics.

Data Cleaning & Preprocessing:
Handled missing values by replacing null entries with default values.
Removed duplicate records to ensure data accuracy.
Ensured data consistency by filling empty fields with sensible defaults (e.g., budget, gross, rating).

SQL Querying with Hive:
Queried the dataset to derive key statistics:
Number of movies released each year.
Distribution of movies by genre.
Movies with the highest votes.
Writers and stars with the most movies or votes.
Aggregated and sorted results to identify key trends.

Data Analysis:
Genre Analysis: Investigated average ratings and total votes per genre.
Budget vs Gross Analysis: Examined country-wise and genre-wise trends in budget and gross earnings.
Director Insights: Analyzed the top directors based on average movie ratings.

Visualizations:
Created a variety of plots to visualize data insights:
Bar Plots: For comparing budget vs gross by country and genre.
Scatter Plots: For total gross vs average budget.
Violin Plot: To explore the distribution of movie ratings within each genre over time.
Line Charts: To show trends in average movie scores across different genres over the years.
Correlation Heatmap: To identify relationships between key numerical variables such as score, votes, budget, and gross.
Pie Chart: For the distribution of movies across genres.

Technologies Used:
PySpark: For efficient distributed data processing.
Hive: For executing SQL queries on large datasets.
Matplotlib & Seaborn: For clear and impactful visualizations.
HDFS: For scalable file storage.

Key Insights:
A clear trend in movie production across years and genres.
Genre-specific patterns in budgets, gross earnings, and movie ratings.
Insights into the directors, stars, and writers with the most influence in the dataset.
