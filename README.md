# E-Commerce Data Analysis

A Spark application. This application will take in a generated csv file of mock ecommerce data and aims to answer a series of analytical questions. The processing of data will be done using Spark SQL. Git and GitHub are used for version control management.

## Technologies Used

Spark - version 3.1.3
Scala - version 2.11.12
Hive - version 3.1

## Features
*	Generating output from a .csv file (HDFS) 
*	Analyzing csv file 
*	Features used:
    a.	Scanner Utillity,
    b.	 ListBuffer,
    c.	 Random, 
    d.	Random Shuffle, 
    e.	Visualization Tool: Microsoft Excel
*	Creating visualizing tool that display the analytics graphically that provide clear answers to the questions that the marketing department asked:
    a.	What is the top selling category of items? Per Country? 
    b.	 How does the popularity of products change throughout the year? Per Country?
    c.	Which locations see the highest traffic of sales? 
    d.	What times have the highest traffic of sales? Per Country?

## To-do list:
*	Analyzing rouge data and finding ways to minimize it
*	Integrate more relative data to our database in order to have a full analysis including factors that affect data.
*	Analyzing the causes of empty spots
*	Analyzing individual items and observing relationships between other columns

## Git Commands Used:
To clone original branch: git clone URL
To create a new branch: git branch Name
To add new changes: git add .
To commit changes: git commit -m “Message”
To push changes to GitHub: git push origin Name

## Usage
In VSCode:
*	Run sbt in folder with build.sbt
*	Then  run
In Spark-Shell:
*	Copy file to hdfs home:
*	Read file  
*	Create a temp view:

## Contributors

Gerald Amory
Julia Berezina
Sam Birk
Tammy Huynh
Carlos Quarterman

