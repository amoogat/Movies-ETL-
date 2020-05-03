#!/usr/bin/env python
# coding: utf-8

# In[1]:


# imports dependencies 
import json
import pandas as pd
import numpy as np
# imports regex expressions 
import re
# imports sql dependencies
from sqlalchemy import create_engine
from config import db_password
import sys
get_ipython().system('{sys.executable} -m pip install psycopg2-binary')
import time


# In[2]:


# loads file dir for easier access to this directory
file_dir = 'C:/Users/Aarian/Desktop/bootcamp/Week8/Movies-ETL-'


# In[3]:


def automated(wiki, kaggle, rating):
    #opens file
    with open(f'{file_dir}/wikipedia.movies.json', mode = 'r') as file:
        wiki_movies_raw = json.load(file)
    # makes data frame for the wiki movies 
    wiki_movies_df = pd.DataFrame(wiki_movies_raw)
    wiki_movies = [movie for movie in wiki_movies_raw if 
               ('Director' in movie or 'Directed by' in movie)
               and 'imdb_link' in movie and 'No. of episodes' not in movie]
    def clean_movie(movie):
        movie = dict(movie) # makes a copy locally
        alt_titles = {}
        # combines alternate titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune-Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
        # merges column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')
        return movie
    # sets new list to a dataframe using list comprehension
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)
    # finding the imdb links
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset = 'imdb_id', inplace = True)
    # gets rid of columns that have less than 90% null values (columns that give the bulk of data)
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    # drops the movies that have null in the box office category
    box_office = wiki_movies_df['Box office'].dropna()
    # creates function to tell if something is a string
    def is_not_a_string(x):
        return type(x) != str
    box_office[box_office.map(is_not_a_string)]
    box_office[box_office.map(lambda x: type(x) != str)]
    # joins list with a space between values
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    # regex expression
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex = True)
    matches_form_one = box_office.str.contains(form_one, flags = re.IGNORECASE)
    matches_form_two = box_office.str.contains(form_two, flags = re.IGNORECASE)
    # gets the data that doesnt match these forms
    box_office[~matches_form_one & ~matches_form_two]
    # extracts form one and two from data
    box_office.str.extract(f'({form_one}|{form_two})')
    def parse_dollars(s):
        # returns NaN for non strings
        if type(s) != str:
            return np.nan

        # checks if input is of form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags = re.IGNORECASE):

            # removes dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]', '', s)

            # converts to float and multiply by a million
            value = float(s) * 10**6

            # returns value
            return value

        # checks if input is of form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags = re.IGNORECASE):

            # removes $ and billion
            s = re.sub('\$|\s|[a-zA-Z]', '', s)

            # converts to float and multiply by a billion
            value = float(s) * 10**9

            # returns value
            return value

        # checks if $###,###,### is the form
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags = re.IGNORECASE):

            # removes dollar sign and commas
            s = re.sub('\$|,', '', s)

            # converts to float
            value = float(s)

            # returns the value
            return value

        # returns nan else
        else:
            return np.nan
    
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags = re.IGNORECASE)[0].apply(parse_dollars)
    # drops the box office column
    wiki_movies_df.drop('Box office', axis = 1, inplace = True)
    # drops nans in budget column and joins on a space, then removes values btw dollar sign and hyphens
    budget = wiki_movies_df['Budget'].dropna()
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex = True)
    matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
    matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
    budget[~matches_form_one & ~matches_form_two]
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    budget[~matches_form_one & ~matches_form_two]
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags = re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Budget', axis = 1, inplace = True)
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    # parse the forms for types of release dates
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    # extracts data and puts it into the forms that were created above
    release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags = re.IGNORECASE)
    # uses to _datetime function to parse the dates
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format = True)
    # non null values of the release dates converted to string
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    # extracts accounting for all formatting with regex
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    # lambda function to convert hour capture groups and minute capture groups to minutes
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis = 1)
    wiki_movies_df.drop('Running time', axis = 1, inplace = True)
    # creates directories for the files
    kaggle_metadata = pd.read_csv(f'{file_dir}/movies_metadata.csv', low_memory = False)
    ratings = pd.read_csv(f'{file_dir}/ratings.csv')
    # removes bad data
    kaggle_metadata[~kaggle_metadata['adult'].isin(['True','False'])]
    # drops rows where adult is true, for non adult movies
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis = 'columns')
    # we want where the video is true
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    # concverts types over
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors = 'raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors = 'raise')
    # converts release date to datetime
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    # converts to datetime (the timestamp)
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit = 's')
    # merges the data of wikipedia and kaggle
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes = ['_wiki','_kaggle'])
    # finds particular rows where titles dont match
    movies_df[movies_df['title_wiki'] != movies_df['title_kaggle']][['title_wiki','title_kaggle']]
    # outliers
    movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')]
    # two movies were merged, dropping
    movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index
    # dropping 
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
    # converts the lists in 'language' so we can use value counts method
    movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna = False)
    # drops the columns we dont need
    movies_df.drop(columns = ['title_wiki','release_date_wiki','Language','Production company(s)'], inplace = True)
    # makes a function to fill in missing data and drop that column to save time
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
            , axis = 1)
        df.drop(columns = wiki_column, inplace = True)
    # uses function that was just created on other columns
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    # checks the count of video column
    movies_df['video'].value_counts(dropna = False)
    # reorders the columns
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                           'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                           'genres','original_language','overview','spoken_languages','Country',
                           'production_companies','production_countries','Distributor',
                           'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                          ]]
    # renames the columns
    movies_df.rename({'id':'kaggle_id',
                      'title_kaggle':'title',
                      'url':'wikipedia_url',
                      'budget_kaggle':'budget',
                      'release_date_kaggle':'release_date',
                      'Country':'country',
                      'Distributor':'distributor',
                      'Producer(s)':'producers',
                      'Director':'director',
                      'Starring':'starring',
                      'Cinematography':'cinematography',
                      'Editor(s)':'editors',
                      'Writer(s)':'writers',
                      'Composer(s)':'composers',
                      'Based on':'based_on'
                     }, axis = 'columns', inplace = True)
    # groups the movies by ratings and counts how many times a movie got a rating
    rating_counts = ratings.groupby(['movieId','rating'], as_index = False).count()                     .rename({'userId':'count'}, axis = 1)                     .pivot(index = 'movieId',columns = 'rating', values = 'count')
    # renames columns by adding rating_ before the column name
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    # left merge on the kaggle id for ratings
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on = 'kaggle_id', right_index = True, how = 'left')
    # fills the missing values with 0s
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    # connection string for sql
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    # creates the database engine
    engine = create_engine(db_string)
    #saves the dataframe movies_df to a table called 'movies_automated'
    movies_df.to_sql(name = 'movies_auto', con = engine)
    # divides data into chunks for importing
    rows_imported = 0
    # get the start_time
    start_time = time.time()
    # breaks into chunks, updates, and writes data to sql data table 'ratings'
    for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize = 1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end = '')
        data.to_sql(name = 'ratings_auto', con = engine, if_exists = 'append')
        rows_imported += len(data)

        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')
    


# In[ ]:


automated('wikipedia.movies.json','movies_metadata.csv','ratings.csv')


# In[ ]:




