#!/usr/bin/env python
# coding: utf-8

# # Prerequisites

# In[1]:


# get data for labs
get_ipython().system('wget -nc -O around_the_world_in_80_days.txt https://www.gutenberg.org/ebooks/103.txt.utf-8')


# # 1. Word Count

# Instructions:  
# For each cell marked "double-click and add explanation here" please answer the question in your own words.  
# In the section where you complete the code to perform basic nlp text cleaning and exploration tasks, the goal is to chain all of the transformations together in a single function. For learning and exploration purposes, it is acceptable to have each step seperate, but the last cell in this section should be one function with all transformations chained together.  
# For steps c and f, it is acceptable to use your favorite chatbot to generate a list of common stop words (c) and punctuation (e) for use in the code. As these are common steps in nlp/text processing tasks, there are pleanty of libraries to help with this such as nltk, but there is no need to import extra dependencies for this lab unless you are already familiar with working with them.

# In[2]:


# start a spark session and create spark context for making rdd
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("word_count") \
    .getOrCreate()

sc = spark.sparkContext


# In[3]:


# Defind the rdd
rdd = sc.textFile('around_the_world_in_80_days.txt')


# In[4]:


# view the first x lines of the rdd
rdd.take(20)


# In[5]:


# example lambda function
words = rdd.flatMap(lambda lines: lines.split(' '))


# In[6]:


# Note and explain the output of the below command
words


# `words` is not a list of words: it is an RDD object (`PythonRDD[...]`). `flatMap` is a **transformation**, and transformations are **lazy**: Spark only records the step, nothing is computed yet.

# In[7]:


# Note and explain the output of the following command, focusing on the difference with the
# above command
words.collect()


# `collect()` is an **action**: Spark now reads the file, runs the `flatMap` and sends all the words back to the driver as a Python list. On a big dataset `collect()` should be avoided (everything goes into the driver memory), `take(n)` is safer.

# In[8]:


# nicer print (only the first 20 words, the book has about 70,000)
for w in words.collect()[:20]:
    print(w)


# In[9]:


# Print first x words
words.take(20)


# In[10]:


get_ipython().run_cell_magic('time', '', '# Use cell magic command to help understand what the rdd.flatMap function is doing in the next cell.\n# Insert a text/markdown cell and explain in your own words.\nprint(rdd.map(lambda line: line.split(\' \')).take(3))\nprint(rdd.flatMap(lambda line: line.split(\' \')).take(3))\nprint(rdd.count(), "lines ->", words.count(), "words")\n')


# `map` returns **one element per line** (a list of words). `flatMap` **flattens** these lists: it returns **one element per word**. This is why the RDD goes from 8,312 lines to 68,577 words. `%%time` shows the time of the cell.

# In[11]:


# Initialize a word counter by creating a tuple with word and cound of 1
words = rdd.flatMap(lambda lines: lines.split(' ')) \
                    .map(lambda word: (word, 1))

for w in words.collect()[:20]:
    print(w)


# In[12]:


# a. count the occurence of each word
word_counts = words.reduceByKey(lambda a, b: a + b)

word_counts.take(10)


# In[13]:


# b. a common first step in text analysis, change all capital letters to lower case
lower_counts = words.map(lambda pair: (pair[0].lower(), pair[1])) \
                    .reduceByKey(lambda a, b: a + b)

lower_counts.take(10)


# In[14]:


# c. eliminate the stop words.
stop_words = {
    "a", "about", "after", "again", "all", "am", "an", "and", "any", "are",
    "as", "at", "be", "been", "before", "but", "by", "can", "could", "did",
    "do", "does", "for", "from", "had", "has", "have", "he", "her", "here",
    "him", "his", "how", "i", "if", "in", "into", "is", "it", "its", "me",
    "more", "my", "no", "not", "now", "of", "on", "one", "only", "or",
    "other", "our", "out", "over", "said", "she", "should", "so", "some",
    "than", "that", "the", "their", "them", "then", "there", "these",
    "they", "this", "those", "to", "too", "up", "upon", "very", "was", "we",
    "were", "what", "when", "where", "which", "while", "who", "whom", "why",
    "will", "with", "would", "you", "your"
}

no_stop_words = lower_counts.filter(lambda pair: pair[0] not in stop_words)

no_stop_words.take(10)


# In[15]:


# d. sort in alphabetical order
no_stop_words.sortByKey().take(10)


# In[16]:


# e. sort descending by word frequency
no_stop_words.sortBy(lambda pair: pair[1], ascending=False).take(10)


# In[17]:


# f. remove punctuations and blank spaces
import re

clean_counts = no_stop_words \
    .map(lambda pair: (re.sub(r"[^\w]", "", pair[0]), pair[1])) \
    .filter(lambda pair: pair[0] != "") \
    .filter(lambda pair: pair[0] not in stop_words) \
    .reduceByKey(lambda a, b: a + b)

clean_counts.sortBy(lambda pair: pair[1], ascending=False).take(10)


# `[^\w]` removes every character that is not a letter or a digit (punctuation, quotes like “ ”). Before this step, `fogg` and `fogg,` were counted as 2 different words, and the empty string `''` was the most frequent "word". The stop words are filtered again, because some were hidden by punctuation (`“i`, `and,`).
# 
# Final function: all the steps chained. Cleaning is done **before** `reduceByKey`, so only one shuffle is needed.

# In[18]:


def word_count(lines, stop_words, alphabetical=False):
    counts = (lines
              .flatMap(lambda line: line.split(' '))
              .map(lambda word: re.sub(r"[^\w]", "", word.lower()))
              .filter(lambda word: word != "" and word not in stop_words)
              .map(lambda word: (word, 1))
              .reduceByKey(lambda a, b: a + b))
    if alphabetical:
        return counts.sortByKey()
    return counts.sortBy(lambda pair: pair[1], ascending=False)


word_count(rdd, stop_words).take(20)


# # 2. What does the following cell block do?
# Comment the code below line by line after the provided hash-tag. You should be able to explain each line while respecting the pep8 style guide of 79 characters or less per line!

# In[19]:


 # Create an RDD of tuples (name, age)
dataRDD = sc.parallelize([("Brooke", 20), ("Denny", 31), ("Jules", 30),
("TD", 35), ("Brooke", 25)])

# Try to undestand what this code does (line by line)
agesRDD = (dataRDD
  # (name, age) -> (name, (age, 1)): the 1 is used to count the records
  .map(lambda x: (x[0], (x[1], 1)))
  # same name: add the ages together and add the counters together
  .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
  # (name, (total_age, count)) -> (name, total_age / count) = average age
  .map(lambda x: (x[0], x[1][0]/x[1][1])))

agesRDD.collect()


# The code computes the **average age per name**. Brooke appears twice (20 and 25), so her average is 22.5.

# ## 3. Function timing.
# 
# - write a simple python timer function for seeing how quickly your rdd runs as written. change the order of the steps in order to make the rdd run as optimally as possible
# 

# In[20]:


import time


def timer(pipeline, repeat=3):
    # run the pipeline several times and return the average time
    times = []
    for _ in range(repeat):
        start = time.perf_counter()
        pipeline().take(20)  # an action is needed, RDDs are lazy
        times.append(time.perf_counter() - start)
    return sum(times) / repeat


# In[21]:


def pipeline_as_written():
    # order of the exercise: count first, clean after (3 shuffles)
    return (rdd
            .flatMap(lambda line: line.split(' '))
            .map(lambda word: (word, 1))
            .reduceByKey(lambda a, b: a + b)
            .map(lambda pair: (pair[0].lower(), pair[1]))
            .reduceByKey(lambda a, b: a + b)
            .map(lambda pair: (re.sub(r"[^\w]", "", pair[0]), pair[1]))
            .reduceByKey(lambda a, b: a + b)
            .filter(lambda pair: pair[0] != "")
            .filter(lambda pair: pair[0] not in stop_words)
            .sortBy(lambda pair: pair[1], ascending=False))


def pipeline_optimized():
    # clean and filter first, then count (1 shuffle)
    return word_count(rdd, stop_words)


print("as written:", round(timer(pipeline_as_written), 3), "s")
print("optimized :", round(timer(pipeline_optimized), 3), "s")
print("same result:", set(pipeline_as_written().collect())
      == set(pipeline_optimized().collect()))


# The optimized order is faster because:
# - `map` and `filter` are **narrow** transformations (no data exchange), `reduceByKey` is **wide** (shuffle). Cleaning before counting needs only 1 `reduceByKey` instead of 3.
# - Filtering the stop words early means less data to shuffle.
# 
# The results are the same, only the order of the steps changes.

# ## 4. Text Comparison
# 
# - perform eda on the original french version of the [book](https://www.gutenberg.org/ebooks/46541.txt.utf-8) and compare the two

# In[22]:


get_ipython().system('wget -nc -O le_tour_du_monde_en_80_jours.txt https://www.gutenberg.org/ebooks/46541.txt.utf-8')


# In[23]:


def remove_gutenberg_text(lines):
    # keep only the book, between the "*** START" and "*** END" lines
    indexed = lines.zipWithIndex()
    start = indexed.filter(lambda x: x[0].startswith("*** START")).first()[1]
    end = indexed.filter(lambda x: x[0].startswith("*** END")).first()[1]
    return indexed.filter(lambda x: start < x[1] < end).map(lambda x: x[0])


rdd_en = remove_gutenberg_text(rdd)
rdd_fr = remove_gutenberg_text(
    sc.textFile('le_tour_du_monde_en_80_jours.txt'))

stop_words_fr = {
    "a", "à", "ai", "au", "aux", "avait", "avec", "c", "ce", "cela", "ces",
    "cet", "cette", "comme", "d", "dans", "de", "des", "dit", "donc", "du",
    "elle", "en", "est", "et", "été", "était", "être", "il", "ils", "j",
    "je", "l", "la", "le", "les", "leur", "lui", "m", "mais", "me", "même",
    "mon", "n", "ne", "nous", "on", "ou", "où", "par", "pas", "pour",
    "qu", "que", "qui", "s", "sa", "sans", "se", "ses", "si", "son", "sont",
    "sur", "t", "tout", "un", "une", "vous", "y", "plus", "bien", "très",
    "ont", "fait", "avoir", "dont", "alors", "encore"
}

# in French the apostrophe joins 2 words (l'homme), so we split on it too
rdd_fr = rdd_fr.map(lambda line: line.replace("'", " ").replace("’", " "))

counts_en = word_count(rdd_en, stop_words)
counts_fr = word_count(rdd_fr, stop_words_fr)


# In[24]:


def total_words(lines):
    return lines.flatMap(lambda line: line.split()).count()


print("lines          EN:", rdd_en.count(), "| FR:", rdd_fr.count())
print("total words    EN:", total_words(rdd_en), "| FR:", total_words(rdd_fr))
print("distinct words EN:", counts_en.count(), "| FR:", counts_fr.count())


# In[25]:


print("Top 10 EN:", counts_en.take(10))
print("Top 10 FR:", counts_fr.take(10))


# In[26]:


# names are the same in both versions: join on the word
names = ["fogg", "passepartout", "phileas", "fix", "aouda"]

counts_en.join(counts_fr) \
    .filter(lambda pair: pair[0] in names) \
    .collect()


# 
# - The French version is longer: 71,915 words vs 63,334, and has more distinct words (9,379 vs 7,069), because French has more forms for the same word (conjugations, feminine, plural).
# - The most frequent words are the same characters in both versions: Fogg, Passepartout, Phileas, Fix, Aouda.
# - The names appear more often in French (Fogg: 688 vs 601), the English translation probably replaces some of them with "he".
# - `mr` is also in the French top words: Verne uses the English title "Mr." in the original.
