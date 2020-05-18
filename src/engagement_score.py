import sqlite3
import pandas as pd
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt
import sys
import os
import numpy as np

def create_wiki_graph(lightdump_file, article_title, outdir):
    """Create a graph from lightdump of Bytes over time

    Keyword arguments:
    lightdump_file -- location of lightdump.txt file
    article_title -- title of the article
    outdir -- directory to save figure
    """
    timestamps = []
    byte_size = []
    revision_count = 0
    num_reverts = 0
    unique_authors = set()
    start = False

    with open(lightdump_file) as lightdump:
        for line in lightdump:
            temp_line = line.strip().split(" ")

            if start and len(temp_line) == 1:
                start = False
                print("Finished Lightdump Parse => num revisions: " + str(revision_count))
                break

            if start:
                timestamps.append(datetime.strptime(temp_line[0][4:], '%Y-%m-%dT%H:%M:%SZ'))
                byte_size.append(int(temp_line[3]))
                revision_count += 1
                num_reverts += int(temp_line[1])
                unique_authors.add(temp_line[4])

            if not start and temp_line[0] == article_title:
                start = True
                print("Starting Lightdump Parse for: " + article_title)
    
    #create dataframe from collected data then plot it
    anarchism = pd.DataFrame({"timestamp": timestamps[::-1], "byte size": byte_size[::-1]})

    anarchism_plot = sns.lineplot(x='timestamp', y='byte size', data=anarchism)

    if not os.path.exists(outdir):
        os.makedirs(outdir, exist_ok=True)
                
    plt.title(article_title + " Page Byte Size Over Time")
        
    fig = anarchism_plot.get_figure()
    fig.savefig(outdir + '/' + article_title + '.png')
    print("Saving plot to \"" + outdir + '/' + article_title + '.png\"')

    plt.show()
    return (fig)

def lightdump_to_db(lightdump_file, outfile, num_articles=3000):
    """Convert Lightdump to DB file averaging across Months

    Keyword arguments:
    lightdump_file -- location of lightdump.txt file
    outfile -- location of output DB file
    num_articles -- number of articles to process from lightdump
    """
    timestamps = []
    byte_size = []
    revision_count = 0
    num_reverts = 0
    unique_authors = set()
    start = False
    editor = []
    article_titles = []
    article_count = 0
    
    # Connect to SQL Table
    conn = sqlite3.connect(outfile)
    c = conn.cursor()
    
    df = pd.DataFrame(columns=['timestamp', 'avg_byte_size', 'num_editor', 'nunique_editors', 'article_title'])
    df.to_sql('ARTICLES', conn, if_exists='replace', index=False)

    with open(lightdump_file) as lightdump:
        for line in lightdump:
            temp_line = line.strip().split(" ")

            if start and len(temp_line) == 1:
                start = False
                article = pd.DataFrame({"article": article_title, "timestamp": timestamps[::-1], "byte_size": byte_size[::-1], "editor": editor[::-1]})
                article.index = article['timestamp']
                if len(article) == 0:
                    timestamps = []
                    byte_size = []
                    editor = []
                    num_reverts = 0
                    revision_count = 0
                    start = True
                    article_title = temp_line[0]
                    continue
                grouped_article = article.groupby(pd.Grouper(freq='M')).agg({'byte_size' : np.mean, 'editor' :['count', 'nunique']})
                grouped_article['article_title'] = [article_title for x in range(len(grouped_article))]
#                 grouped_article.dropna(axis="rows", inplace=True)
                grouped_article.columns = ['avg_byte_size', 'num_editor', 'nunique_editors', 'article_title']
        
                grouped_article['timestamp'] = grouped_article.index
                
                # Write to SQL Table
                grouped_article.to_sql('ARTICLES', conn, if_exists='append', index=False)
            
                print(str(article_count) + ": FINISHED LIGHTDUMP PARSE FOR:   " + str(article_title) + "\t\t NUM REVISIONS: " + str(revision_count))
                num_reverts = 0
                revision_count = 0
                article_count += 1
                timestamps = []
                byte_size = []
                editor = []
                if article_count > num_articles:
                    break

            if start:
                timestamps.append(datetime.strptime(temp_line[0][4:], '%Y-%m-%dT%H:%M:%SZ'))
                byte_size.append(int(temp_line[3]))
                revision_count += 1
                num_reverts += int(temp_line[1])
                editor.append(temp_line[4])
                article_titles.append(article_title)

            if not start:
                start = True
                article_title = temp_line[0]
    
    print("Exporting to DB File")
    conn.commit()


def selectArticlesDB(dbfile, article_titles):
    """Select Articles from DB File

    Keyword arguments:
    dbfile -- location of db file
    article_titles -- list of articles in query
    """
    conn = sqlite3.connect(dbfile)
    c = conn.cursor()
    sql_query = "SELECT * FROM ARTICLES"
    
    for i in range(len(article_titles)):
        if i == 0:
            sql_query += " WHERE "
        else:
            sql_query += " OR "
        sql_query += "article_title=\"" + article_titles[i] + "\""
        
    print(sql_query)
    
    c.execute(sql_query)
    
    df = pd.DataFrame(c.fetchall(), columns=['timestamp', 'avg_bytes', 'edits', 'nunique_editors', 'article_title'])    
    df['edits'] = [int(x) for x in df['edits']]
    df['avg_bytes'] = [0 if x is None else float(x) for x in df['avg_bytes']]
    df['nunique_editors'] = [int(x) for x in df['nunique_editors']]
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

def editor_engagement_score(df, outdir="./data/out"):
    """ calculate the editor engagement score given a dataframe from lightdump parsing
    
    Keyword arguments:
    df -- dataframe with articles averaged over month
    outdir -- outdir for the csv file output
    """
    diff_edits = [df['edits'][x] - df['edits'][x-1] for x in range(1, len(df))]
    diff_edits.insert(0,0)
    df['diff_edits'] = diff_edits
    diff_nunique = [df['nunique_editors'][x] - df['nunique_editors'][x-1] for x in range(1, len(df))]
    diff_nunique.insert(0, 0)
    df['diff_nunique'] = diff_nunique
    df['product'] = df['diff_edits'] * df['diff_nunique']
    df.to_csv(outdir + "/engagement_scores.csv', index=False)

    return df[['timestamp', 'diff_edits', 'diff_nunique', 'product']]