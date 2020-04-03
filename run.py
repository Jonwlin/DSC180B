#!/usr/bin/env python

import sys
import json
import shutil
import pandas as pd
import os

import warnings
warnings.filterwarnings("ignore")

sys.path.insert(0, 'src') # add library code to path
from etl import process_lightdump
from etl import parse_enwiki_to_lightdump
from etl import download_enwiki_zips
from etl import extract_7zip
from m_score import mscore_over_time

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

    #test data process analysis

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
        #download enwiki files
        zip_outdir = outdir + "/raw/zips"
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

        #download enwiki files
        zip_outdir = outdir + "/raw/zips"
        download_enwiki_zips(num_files_to_download, zip_outdir , False)

        zips = os.listdir(zip_outdir)

        #extract enwiki files
        extract_outdir = outdir + "/raw/extracted"

        for zip_file in os.listdir(zip_outdir):
            if zip_file[-2:] == "7z":
                extract_7zip(zip_outdir + "/" + zip_file, extract_outdir)

        extracted = os.listdir(extract_outdir)

        #parse enwiki to lightdump
        file_to_parse = extract_outdir + "/enwiki-20200101-0"
        lightdump_filename = "lightdump.txt"
        temp_dir = outdir + "/temp"

        print("Parsing wikidump to lightdump")

        articles = test_params['articles']

        parse_enwiki_to_lightdump(file_to_parse , lightdump_filename, temp_dir, articles)

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
    return


if __name__ == '__main__':
    targets = sys.argv[1:]
    main(targets)
