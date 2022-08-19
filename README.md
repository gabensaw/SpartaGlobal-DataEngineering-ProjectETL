
# Data-31-Project-ETL

This project contains resources which creates an ETL pipeline to track a progress of an invidivual (Spartan) who is/was part of the Sparta Global Academy .

The pipeline contains data from an Amazon S3 data lake which includes:

1) Academy Data - Consists of the Spartan trainer and the weekly scored competency results Spartans have received.

2) Talent Data - Details of applicants who have been invited to attend a Sparta Assessment day and the day they will be attending

3) Sparta Assessment day - Presentation and Psychometric test results of individuals

4) Applicant review -  Overall performance results of individuals who attended the Sparta Assessment day

Extracted Data is then transformed within our program and loaded into PostgreSQL for querying

# Prerequisites

Latest version of PostgreSQL

Latest version of Python

# Running this project

To get this project up and running you should start by having Python installed on your computer. It's advised you create a virtual environment to store your projects dependencies separately. You can install virtualenv with

``` pip install virtualenv ```

Clone or download this repository and open it in your editor of choice. In a terminal (mac/linux) or windows terminal, run the following command in the base directory of this project
``` virtualenv env ```
 
That will create a new folder env in your project directory. Next activate it with this command on mac/linux:

``` source env/bin/active ```

Then install the project dependencies with

``` pip install -r requirements.txt ```
