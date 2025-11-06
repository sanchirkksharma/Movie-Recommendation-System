xxxxxx------------------------xxxxxx--------------------xxxxxx--------------------xxxxxx-------------xxxxxx
                                                                                                                                      ### Movie Recommendation Engine using Apache Spark on AWS EMR
                                                                                                         
xxxxxx------------------------xxxxxx--------------------xxxxxx--------------------xxxxxx-------------xxxxxx

## Project Overview
This project implements a movie-to-movie recommendation system using Apache Spark (Scala), AWS EMR, and the MovieLens 1M dataset.
The system computes cosine similarity between movie pairs based on user ratings to identify and recommend similar movies, all at scale on a distributed cluster.

## Key Features
* Processes 1 million+ movie ratings using Spark’s distributed computation framework.
* Uses Cosine Similarity to compute pairwise similarity between movies.
* Deployed on an AWS EMR cluster (3-node setup) leveraging S3 as a distributed data store.
* Outputs top 50 most similar movies for a given target movie (e.g., Star Wars).
* Demonstrates scalable, production-style Big Data Engineering with real datasets.

## Technologies Used
* Compute Engine       -->	  Apache Spark (Scala)
* Cloud Platform       -->	  AWS Elastic MapReduce (EMR)
* Storage	           -->    Amazon S3
* Programming Language -->	  Scala 2.11
* Cluster Runtime	   -->    Spark 2.4.x on YARN
* Access Tool	       -->    PuTTY (SSH client for Windows), EC2 Key Pair
