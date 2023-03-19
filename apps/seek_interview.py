#!/usr/bin/env python
# coding: utf-8

# In[1]:


## Download Test Data
get_ipython().system('wget -q https://coding-challenge-public.s3.ap-southeast-2.amazonaws.com/test-data.zip')


# In[2]:


## Unzip Test Data
get_ipython().system(' unzip  -o  -P By9FNTZXp4j4izuufAs= ~/test-data.zip -d ~/')


# In[3]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("seek_interview").getOrCreate()


# In[4]:


spark


# In[6]:


# 1. Load the dataset into a Spark dataframe.
# 2. Print the schema
# 3. How many records are there in the dataset?

test_data_df = spark.read.json("test_data/*.json")
number_of_rows = test_data_df.count()
test_data_df.printSchema()
print("There's %d rows in Test Data"%number_of_rows)


# In[7]:


# 4. What is the average salary for each profile? Display the first 10 results, ordered by lastName in descending order.
test_data_df.createOrReplaceTempView("test_data_tab")
#q4=spark.sql("select id,profile.firstName,profile.lastName,profile.jobHistory from test_data_tab limit 5").show(5)
q1=spark.sql(
            "Select id, \
                    firstName, \
                    lastName, \
                    avg(jobHistory.salary) salary \
            from \
                (select id, \
                    profile.firstName,\
                    profile.lastName, \
                    explode(profile.jobHistory) as jobHistory \
                 from test_data_tab \
                 ) \
            group by 1,2,3 \
            order by lastName desc \
            limit 10"
            ).show()


# In[8]:


#5. What is the average salary across the whole dataset?
q5=spark.sql(
            "Select avg(jobHistory.salary) salary \
            from \
                (select id, \
                    profile.firstName,\
                    profile.lastName, \
                    explode(profile.jobHistory) as jobHistory \
                 from test_data_tab \
                 ) \
            "
            ).show()


# In[9]:


#6. On average, what are the top 5 paying jobs? Bottom 5 paying jobs? If there is a tie, please order by title, location.
q6_1=spark.sql(
            "Select title, \
                    salary, \
                    location, \
                    'High Paying jobs' Category, \
                    row_number() over (order by (salary)  desc , title asc, location asc) rnk \
            from (\
                  Select distinct jobHistory.title, \
                        jobHistory.salary, \
                        jobHistory.location \
            from (\
                    select id, \
                        profile.firstName,\
                        profile.lastName, \
                        explode(profile.jobHistory) as jobHistory \
                     from test_data_tab \
                 ) \
                ) \
            order by rnk asc \
            limit 5"
            ).show(5,False)

q6_2=spark.sql(
            "Select title, \
                    salary, \
                    location, \
                    'Low Paying jobs' Category, \
                    row_number() over (order by (salary)  asc , title asc, location asc) rnk \
            from (\
                  Select distinct jobHistory.title, \
                        jobHistory.salary, \
                        jobHistory.location \
            from (\
                    select id, \
                        profile.firstName,\
                        profile.lastName, \
                        explode(profile.jobHistory) as jobHistory \
                     from test_data_tab \
                 ) \
                ) \
            order by rnk asc \
            limit 5"
            ).show(5,False)


# In[10]:


#7. Who is currently making the most money? If there is a tie, please order in lastName descending, fromDate descending.
q7=spark.sql(
            "Select firstName, \
                    lastName, \
                    jobHistory.title, \
                    jobHistory.fromDate, \
                    max(jobHistory.salary) avg_salary, \
                    row_number() over ( order by max(jobHistory.salary)  desc , lastName desc, jobHistory.fromDate desc) rnk \
            from \
                (select id, \
                    profile.firstName,\
                    profile.lastName, \
                    explode(profile.jobHistory) as jobHistory \
                 from test_data_tab \
                 ) \
            group by 1,2,3,4 \
            order by rnk asc \
            limit 1"
            ).show(1,False)


# In[11]:


#8. What was the most popular job title that started in 2019?
q8=spark.sql(
            "Select jobHistory.title, \
                    row_number() over ( order by count(distinct id)  desc) rnk \
            from \
                (select id, \
                    profile.firstName,\
                    profile.lastName, \
                    explode(profile.jobHistory) as jobHistory \
                 from test_data_tab \
                 ) \
            where year(jobHistory.fromDate)=2019 \
            group by 1 \
            order by rnk asc \
            limit 1"
            ).show(1,False)


# In[12]:


#9. How many people are currently working?
q9=spark.sql(
            "Select count(distinct id) current_workers_count \
            from \
                (select id, \
                    profile.firstName,\
                    profile.lastName, \
                    explode(profile.jobHistory) as jobHistory \
                 from test_data_tab \
                 ) \
            where jobHistory.toDate is null\
            limit 1"
            ).show(1,False)


# In[13]:


#10. For each person, list only their latest job. Display the first 10 results, ordered by lastName descending, firstName ascending order.
q10=spark.sql(
            "Select firstName, \
                    lastName, \
                    title \
            from( Select id, \
                    firstName, \
                    lastName, \
                    jobHistory.title, \
                    row_number() over (partition by id order by coalesce(jobHistory.toDate,'9999') desc) rnk \
                from \
                    (select id, \
                        profile.firstName,\
                        profile.lastName, \
                        explode(profile.jobHistory) as jobHistory \
                     from test_data_tab \
                     ) \
                ) \
            where rnk=1 \
            order by lastName desc, firstName asc \
            limit 10"
            ).show(10,False)


# In[14]:


#11. For each person, list their highest paying job along with their first name, last name, salary and the year they made this salary. 
#    Store the results in a dataframe, and then print out 10 results

q11=spark.sql(
            "Select firstName, \
                    lastName, \
                    salary ,\
                    high_sal_job_year \
            from( Select id, \
                    firstName, \
                    lastName, \
                    jobHistory.salary, \
                    year(jobHistory.fromDate) high_sal_job_year, \
                    row_number() over (partition by id order by jobHistory.salary desc) rnk \
                from \
                    (select id, \
                        profile.firstName,\
                        profile.lastName, \
                        explode(profile.jobHistory) as jobHistory \
                     from test_data_tab \
                     ) \
                ) \
            where rnk=1 \
            order by lastName desc, firstName asc \
            "
            )
q11.show(10,False)


# In[15]:


#12. Write out the last result (question 11) in parquet format, compressed, partitioned by the year of their highest paying job.
q11.write.partitionBy("high_sal_job_year").mode('overwrite').option("compression","gzip").parquet("q11.parquet")


# In[ ]:




