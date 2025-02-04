# Data-Analysis-on-Movies-dataset-using-Hadoop-Hive-and-PySpark
This is my first ever project related to data analysis and I am proud of it. I sat day and night for four days to set up the environment and integrate Hadoop, hive and spark. This has been hard for me as I have no experience in data analysis and had no knowledge of PySpark before this. This project made me believe in myself once again. This is a small project considering the perspective of a technical expert but this project that I did in my first semester, for the fall of 2023 is what gave me confidence that I can do anything if I set my mind to it. 
The source code included the original code that I used for the project. I set the environment on my local machine; detailed documentation on the same can be found in the presentation file. 
Many times, I wished for relaxation regarding where we could get the data file. I might have used Amazon S3 bucket as I felt that was a way easier process. However, the entire project journey only made me believe in myself, so I have no regrets.

This project analyzes a movie dataset using PySpark for data processing, Hive for SQL queries, and Matplotlib and Seaborn for data visualization. The objective is to extract meaningful insights about movies, genres, budgets, and other key metrics.

**Data Cleaning & Preprocessing:**
Handled missing values by replacing null entries with default values.
Removed duplicate records to ensure data accuracy.
Ensured data consistency by filling empty fields with sensible defaults (e.g., budget, gross, rating).

**SQL Querying with Hive:**
Queried the dataset to derive key statistics:
Number of movies released each year.
Distribution of movies by genre.
Movies with the highest votes.
Writers and stars with the most movies or votes.
Aggregated and sorted results to identify key trends.

**Data Analysis:**
Genre Analysis: Investigated average ratings and total votes per genre.
Budget vs Gross Analysis: Examined country-wise and genre-wise trends in budget and gross earnings.
Director Insights: Analyzed the top directors based on average movie ratings.

**Visualizations:**
Created a variety of plots to visualize data insights:
Bar Plots: For comparing budget vs gross by country and genre.
Scatter Plots: For total gross vs average budget.
Violin Plot: To explore the distribution of movie ratings within each genre over time.
Line Charts: To show trends in average movie scores across different genres over the years.
Correlation Heatmap: To identify relationships between key numerical variables such as score, votes, budget, and gross.
Pie Chart: For the distribution of movies across genres.

**Technologies Used:**
PySpark: For efficient distributed data processing.
Hive: For executing SQL queries on large datasets.
Matplotlib & Seaborn: For clear and impactful visualizations.
HDFS: For scalable file storage.

**Results:**
![Screenshot 2023-11-29 235105](https://github.com/user-attachments/assets/9e37030d-d032-4431-99c9-d250429c8c8a)
![Screenshot 2023-11-29 235124](https://github.com/user-attachments/assets/3b8dd694-bb52-433c-8564-73267e48f3c3)
![Screenshot 2023-11-29 235201](https://github.com/user-attachments/assets/efdf9142-c1f1-45d5-95e3-ea124cb6fb96)
![Screenshot 2023-11-29 235237](https://github.com/user-attachments/assets/69c1a2b5-c873-4c6d-a141-2fe0a9acc93b)
![Screenshot 2023-11-30 000257](https://github.com/user-attachments/assets/8854fa63-f855-4d11-b9a2-df474578f21e)
![Screenshot 2023-11-30 000338](https://github.com/user-attachments/assets/238caf70-1307-429f-b4b7-4c9802069a83)
![Screenshot 2023-11-30 000415](https://github.com/user-attachments/assets/37c379ca-bdfd-487b-a5a8-536f7813ed19)
![Screenshot 2023-11-30 000450](https://github.com/user-attachments/assets/318076af-e440-48d3-bf96-acd13d498262)
![Screenshot 2023-11-30 000518](https://github.com/user-attachments/assets/40ead791-c907-4fca-b0bd-a69ee85abf88)
![Screenshot 2023-11-30 000540](https://github.com/user-attachments/assets/0e840ec9-5dba-40a6-afd7-dbbbc06fa504)
![Screenshot 2023-11-30 000603](https://github.com/user-attachments/assets/f50c4078-a81b-4ec2-a048-12a4476dd549)
![Screenshot 2023-11-30 000630](https://github.com/user-attachments/assets/435a686b-f5f4-4b05-b38f-5194d3e6bdb1)
![Screenshot 2023-11-30 000657](https://github.com/user-attachments/assets/807584b8-1b6c-42dc-9592-a2a574303c54)

**Key Insights:**
A clear trend in movie production across years and genres.
Genre-specific patterns in budgets, gross earnings, and movie ratings.
Insights into the directors, stars, and writers with the most influence in the dataset.





