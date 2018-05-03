# Hadoop_InvertedIndex
Implementing Inverted Index on Hadoop cluster using Map-Reduce

**Goal**

Creating an Inverted Index of words occurring in a set of English books.
We’ll be using a collection of 3,036 English books written by 142 authors. This collection is a small subset of the Project Gutenberg corpus.

**Steps carried out**

 Books were placed in a bucket on my Google cloud storage and the Hadoop job was instructed to read the input from this bucket. 

 1. Uploading the input data into the bucket 
 2. Implementing InvertedIndex using Map-Reduce
 3. Creating jar of the code. (The Google Cloud console requires us
to upload a Map-Reduce (Hadoop) job as a jar file)
4. Submitting Hadoop job to our cluster


