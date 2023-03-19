# seek_pyspark_interview

# Steps to follow 
Step 1: git clone https://github.com/sundeepydv/seek_pyspark_interview.git

Step 2: Run below two commands to build and run docker image

            docker build --rm -t jupyter/pyspark-notebook:latest .
            docker run -it --rm -p 8888:8888 jupyter/pyspark-notebook:latest

Step 3: Open jupyterhub notedbook using link provided in the output of previous command.
            http://127.0.0.1:8888/lab?token=XXX

Step 4: You can now open seek_interview.ipynb in Jupyterhub, run and review questions.  (You might have to switch line seperator between LF and CRLF)


## Backlog or Next Steps

1. Can create a python process.py script that reads from yml to pick up sql 
2. Create functions to process json/csv/xml data
3. Modularise code to create seperate functions for 
    a. connecting to spark
    b. reading from source data and create dfs
    c. getting sql from config and running them on source df
    d. parquet file creation

## Spark/SQL Questions addressed as part of this project
1. Load the dataset into a Spark dataframe.
2. Print the schema
3. How many records are there in the dataset?
4. What is the average salary for each profile? Display the first 10 results, ordered by lastName in descending order.
5. What is the average salary across the whole dataset?
6. On average, what are the top 5 paying jobs? Bottom 5 paying jobs? If there is a tie, please order by title, location.
7. Who is currently making the most money? If there is a tie, please order in lastName descending, fromDate descending.
8. What was the most popular job title that started in 2019?
9. How many people are currently working?
10. For each person, list only their latest job. Display the first 10 results, ordered by lastName descending, firstName ascending order.
11. For each person, list their highest paying job along with their first name, last name, salary and the year they made this salary. Store the results in a dataframe, and then print out 10 results
12. Write out the last result (question 11) in parquet format, compressed, partitioned by the year of their highest paying job.

