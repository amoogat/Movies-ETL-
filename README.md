# Movies-ETL-
This repository uses ETL (Extraction, Transformation, Loading) to process data regarding movies from wikipedia and kaggle, and creates a function that can be run in order to update the extraction data as well as the transformation and the loading. 
## Tools
- Python 3.7+
- pgAdmin 4
- pandas for easier extraction and viewing
- psycopg2 for sql loading
- Repo connects SQL using jupyter notebook for python and pg Admin for SQL
## Extraction 
The data is extracted from wikipedia and kaggle, and then merged into one big dataset.
## Transformation
Skimming the columns, there were many extraneous values that were then dropped. After isolating individual columns and figuring out what data was the most relevant (wikipedia or kaggle), the data was further edited or trimmed based on rating, runtime, budget, or revenue. 
## Load 
The data was then uploaded to SQL for end user. Note that ratings.csv is a large file and the code may take some time to run.
