E-Commerce Data Analysis

A Spark application. This application will take in a generated csv file of mock ecommerce data and aims to answer a series of analytical questions. The processing of data will be done using Spark SQL. Git and GitHub are used for version control management.

Technologies Used

Spark - version 3.1.3
Scala - version 2.11.12
Hive - version 3.1

Features
1.	Generating output from a .csv file (HDFS) 
2.	Analyzing csv file 
3.	Features used:
    a.	Scanner Utillity,
    b.	 ListBuffer,
    c.	 Random, 
    d.	Random Shuffle, 
    e.	Visualization Tool: Microsoft Excel
4.	Creating visualizing tool that display the analytics graphically that provide clear answers to the questions that the marketing department asked:
    a.	What is the top selling category of items? Per Country? 
    b.	 How does the popularity of products change throughout the year? Per Country?
    c.	Which locations see the highest traffic of sales? 
    d.	What times have the highest traffic of sales? Per Country?

To-do list:
    1.	Analyzing rouge data and find a way to minimize it
    2.	Integrate more relative data to our database in order to have a full analysis including factors that affect data.
    3.	Analyze empty spots and what it caused.
    4.	Analyze individually items and see relationship between other columns.

Git Commands Used:
To clone original branch: git clone URL
To create a new branch: git branch Name
To add new changes: git add .
To commit changes: git commit -m “Message”
To push changes to GitHub: git push origin Name

Usage
In VSCode:
1.	Run sbt in folder with build.sbt
2.	Then  run
In Spark-Shell:
1.	Copy file to hdfs home:
2.	Read file  
3.	Create a temp view:

Contributors

Gerald Amory
Julia Berezina
Sam Birk
Tammy Huynh
Carlos Quarterman

