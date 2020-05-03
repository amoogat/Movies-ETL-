# Movies-ETL-
This repository uses ETL (Extraction, Transformation, Loading) to process data regarding movies from wikipedia and kaggle, and creates a function that can be run in order to update the extraction data as well as the transformation and the loading. 
## Tools
- Python 3.7+ on jupyter notebook to view challenges.ipynb and movies.ipynb
- pgAdmin 4
- pandas for easier extraction and viewing
- psycopg2 for sql loading
- Repo connects SQL using jupyter notebook for python and pg Admin for SQL
## Extraction 
The raw data is extracted from Wikipedia and Kaggle, and then merged into one big dataset.
## Transformation
Skimming the columns (see Movies.ipynb), there were many extraneous values that were dropped. After isolating individual columns and figuring out what data was the most relevant (wikipedia or kaggle), the data was further edited or trimmed based on rating, runtime, budget, or revenue. 
## Load 
The refined data was then uploaded to SQL for end user. Note that ratings.csv is a large file and the code may take some time to run.
# Assumptions
For the challenge code to work, we are assuming that the data is downloaded into the correct file (need wikipedia, kaggle, and ratings data for the movies). These are pulled from a json and locally downloaded files ratings.csv and movies_metadata.csv. These csv's have not both been uploaded due to the size of ratings.csv being 800 mb. 
