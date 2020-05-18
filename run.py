#!/usr/bin/env python

import sys
import json
import shutil
import pandas as pd
import os

import warnings
warnings.filterwarnings("ignore")

sys.path.insert(0, 'src') # add library code to path
from wikiparser import process_lightdump
from wikiparser import parse_enwiki_to_lightdump
from wikiparser import download_enwiki_zips
from wikiparser import extract_7zip

from wikiparser import gunzip_shutil
from wikiparser import download_metadata_zips
from wikiparser import parse_metadata_to_lightdump

from engagement_score import content_engagement_score
from engagement_score import editor_engagement_score
from engagement_score import get_page_views
from engagement_score import selectArticlesDB
from engagement_score import lightdump_to_db

DATA_PARAMS = 'config/data-params.json'
TEST_PARAMS = 'config/test-params.json'

def load_params(fp):
    with open(fp) as fh:
        param = json.load(fh)
    return param

def main(targets):
    # make the clean target
    if 'clean' in targets:
        print("Cleaning data")
        shutil.rmtree('data/temp', ignore_errors=True)
        shutil.rmtree('data/out', ignore_errors=True)
        
    # take small chunk of data
    if 'test' in targets:
        params = load_params(TEST_PARAMS)
        outdir = params['outdir']
        num_files_to_download = params["num_files_download"]
    else:
        params = load_params(DATA_PARAMS)
        outdir = params['outdir']

    #downloads raw data
    if 'data' in targets:
        
        test_params = load_params(TEST_PARAMS)

        outdir = test_params['outdir']
        zip_outdir = outdir + "/raw/zips"

        num_files_to_download = test_params["num_files_download"]
        
        #download enwiki files
        download_enwiki_zips(num_files_to_download, zip_outdir , False)
        zips = os.listdir(zip_outdir)
        #extract enwiki files
        extract_outdir = outdir + "/raw/extracted"

        for zip_file in os.listdir(zip_outdir):
            if zip_file[-2:] == "7z":
                extract_7zip(zip_outdir + "/" + zip_file, extract_outdir)

    # process enwiki to lightdump
    if "process" in targets:
        #parse enwiki to lightdump
        file_to_parse = extract_outdir + "/enwiki-20200101-0"
        lightdump_filename = "lightdump.txt"
        temp_dir = outdir + "/temp"

        print("Parsing wikidump to lightdump")

        articles = params['articles']

        parse_enwiki_to_lightdump(file_to_parse , lightdump_filename, temp_dir, articles)

    if "analysis" in targets:
        print("Calculating M-Score for Anarchism and Autism")

        #mscore calculations
        mscore_dict = process_lightdump(temp_dir + "/lightdump.txt")

        output_dir = outdir + "/out"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        pd.DataFrame(mscore_dict.items(), columns=['Title', 'MScore']).to_csv(output_dir + "/mscores.csv", index=False)
        #get charts for mscore over time
        for title in articles:
            mscore_over_time( temp_dir + "/lightdump.txt", title, output_dir + "/" + title)
        print("Finished. Outputs are available in {}".format(outdir + "/out"))

    if "test-project" in targets:
        test_params = load_params(TEST_PARAMS)

        outdir = test_params['outdir']

        num_files_to_download = test_params["num_files_download"]

        #Download Metedata EN Wiki Raw
        zip_outdir = "../data/raw/zips"

        download_metadata_zips(1, zip_outdir , False)

        zips = os.listdir(zip_outdir)

        #extract enwiki files
        extract_outdir = outdir + "/raw/extracted"
        extracted = os.listdir(extract_outdir)

        for gzip_file in os.listdir(zip_outdir ):
            if gzip_file[-2:] == "gz":
                gunzip_shutil(zip_outdir + "/" + gzip_file, extract_outdir)

        #parse extracted enwiki to lightdump
        file_to_parse = "./data/raw/extracted/metadata-20200401-1.xml"
        lightdump_filename = "lightdump.txt"
        temp_dir = "./data/temp"

        print("Parsing Wikidump to Lightdump")
        
        articles = test_params['articles']
        
        parse_metadata_to_lightdump(file_to_parse , lightdump_filename, temp_dir, articles)

        print("Getting Page View data for Articles") #Getting page views
                
        pageviews_outfile = temp_dir + "/pageviews.csv"
    
        get_page_views(articles, pageviews_outfile)
        
        lightdump_file = "./data/temp/popularity_dump.txt"
        db_outfile = "./data/out/articles.db"
        lightdump_to_db(lightdump_file, db_outfile, num_articles=3000)

        #Produce outputs now
        output_dir = outdir + "/out"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            
        #calculate editor engagement score and content engagement score
        print("Calculating Content Engagement Score")
        content_engagement_score(pageviews_outfile, db_outfile, output_dir + "/content_engagement.csv")
        
        df = selectArticlesDB(db_outfile, articles)

        print("Calculating Editor Engagement Score")
        editor_engagement_score(df, output_dir)

        print("Finished. Outputs are available in {}".format(output_dir))
    return


if __name__ == '__main__':
    targets = sys.argv[1:]
    main(targets)
