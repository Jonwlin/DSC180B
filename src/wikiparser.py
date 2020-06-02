#### ENWIKI RAW DATA DOWNLOADING AND PARSING ####

import os
      
        
def process_lightdump(filepath_to_lightdump):
    '''
    pass in filepath of unzipped txt file
    returns dataframe of (title, num revert pairs, num mutual reverts, mscore)
    '''
    from m_score import parse_lightdump_mscore
    
    df = parse_lightdump_mscore(filepath_to_lightdump)
    
    #return dictionary of mscores
    return df
               
#Download the zips from wikipedia dump
def download_enwiki_zips(num_files_download, outdir, overwrite=False):
    '''
    download enwiki data dumps, number defines how many
    '''
    import requests
    import urllib.request
    from bs4 import BeautifulSoup
    import os

    url = 'https://dumps.wikimedia.org/enwiki/20200101/'
    response = requests.get(url)

    soup = BeautifulSoup(response.text, "html.parser")
    ul_links = soup.findAll('ul')[3].findAll('a')

    if not os.path.exists(outdir):
        os.makedirs(outdir, exist_ok=True)

    for i in range(num_files_download): #'a' tags are for links
        print("Downloading enwiki file: (" + str(i+1) + "/" + str(num_files_download) + ")")

        link = ul_links[i]['href']
        download_url = os.path.join('https://dumps.wikimedia.org', link[1:])

        filename = os.path.join(outdir, "enwiki-20200101-" + str(i) + ".7z") #generate filename

        #if file name exists then skip, and not overwrite
        if not overwrite and os.path.exists(filename):
            continue

        urllib.request.urlretrieve(download_url, filename) #pass filename to urllib request

def extract_7zip(filepath, outdir):
    import shutil
    import os
    from py7zr import unpack_7zarchive

    try:
        shutil.register_unpack_format('7zip', ['.7z'], unpack_7zarchive)
    except:
        pass

    if not os.path.exists(outdir):
        os.mkdir(outdir)
        
    filename = outdir + "/" + filepath.split("/")[-1][:-3]
    #if file name exists then skip, and not overwrite
    if os.path.exists(filename):
        print("Extracted file already exists: {}".format(filename))
        return

    print("Extracting file: {} --- to path --- {}".format(filepath, outdir))
    shutil.unpack_archive(filepath, outdir)
    
class Revision:
    def __init__(self, timestamp, revert, version, contributor, revision_id, revision_parentid, sha1):
        self.timestamp = timestamp
        self.revert = revert
        self.version = version
        self.contributor = contributor
        self.revision_id = revision_id
        self.revision_parentid = revision_parentid
        self.sha1 = sha1
        
    def __repr__(self):
        return [self.timestamp, self.revert, self.version, self.contributor, self.revision_id, self.revision_parentid]
        
    def __str__(self):
        try:
            return "^^^_" + self.timestamp + " " + str(self.revert) + " " + str(self.version) + " " + self.contributor
        except:
            print(self.timestamp)
            print(self.revert)
            print(self.version)
            print(self.contributor)
    
    def get_revision_id(self):
        return self.revision_id
    
#parse the file, calculate lightdump information, output to outfile
def parse_enwiki_to_lightdump(filepath, outfile, outdir, articles=[]):
    
    from lxml import etree

    context = etree.iterparse(filepath, tag='{http://www.mediawiki.org/xml/export-0.10/}page', encoding='utf-8')
    nsmap = {'ns': 'http://www.mediawiki.org/xml/export-0.10/'}

    article_count = len(articles) if len(articles) != 0 else -1
    
    revi_header = "^^^_"

    page_dicts = {}
        
    if not os.path.exists(outdir):
        os.makedirs(outdir, exist_ok=True)

    with open(outdir + "/" + outfile, 'w') as file:
        file.write("")

    for event, elem in context:

        page_title = '_'.join(elem.find('ns:title', nsmap).text.split())
        page_id = elem.find('ns:id', nsmap).text
        revisions = elem.findall('ns:revision', nsmap)

        rev_dicts = []
        
        if len(articles) != 0 and page_title not in articles:
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            continue
        
#         print(page_title)
#         print(len(revisions))
        
        for revision in revisions:
            rev = {}
            
#             for item in revision:
#                 print(item, item.text)
            
            rev['revision_id'] = revision.find("ns:id", nsmap).text
            rev['revision_parentid'] = revision.find("ns:parentid", nsmap).text if revision.find("ns:parentid", nsmap) != None else None
            rev['timestamp'] = revision.find("ns:timestamp", nsmap).text
            rev['sha1'] = revision.find("ns:sha1", nsmap).text

            #getting contributer info
            contributor_info = revision.find("ns:contributor", nsmap)
            revision_contributor_username = contributor_info.find("ns:username", nsmap).text if contributor_info.find("ns:username", nsmap) != None else None
            revision_contributor_id = contributor_info.find("ns:id", nsmap).text if contributor_info.find("ns:id", nsmap) != None else None
            revision_contributor_ip = contributor_info.find("ns:ip", nsmap).text if contributor_info.find("ns:ip", nsmap) != None else None
            
            if revision_contributor_username != None:
                revision_contributor_username = "_".join(revision_contributor_username.split())
                rev['contributor'] = revision_contributor_username
            elif revision_contributor_ip != None:
                rev['contributor'] = revision_contributor_ip
            else:
                rev['contributor'] = "null"
                
            rev_dicts.append(rev)
            
        rev_dicts.sort(key=lambda x: x['timestamp'])

        version = 1
        page_results = []

        for i in range(len(rev_dicts)):

            temp_rev = Revision(rev_dicts[i]['timestamp'], 0, version, rev_dicts[i]['contributor'], rev_dicts[i]['revision_id'], rev_dicts[i]['revision_parentid'], rev_dicts[i]['sha1'])
            if rev_dicts[i]['revision_parentid'] == None:
                page_results.append(temp_rev)
                version += 1    
            else:
                # find the point we revert to
                min_ind = len(page_results) - 1
                while min_ind > 0:
                    if page_results[min_ind].sha1 == rev_dicts[i]['sha1']:
                        temp_rev.version = page_results[min_ind].version
                        temp_rev.revert = 1
                        page_results.append(temp_rev)
                        break
                    min_ind -= 1

                if min_ind == 0:
                    page_results.append(temp_rev)
                    version += 1
        
        if len(articles) == 0 or page_title in articles:
            article_count -= 1
    
            print("Writing {} {} revisions to lightdump.txt".format(page_title, len(page_results)))
            with open(outdir + "/" + outfile, 'a') as file:
                file.write(page_title.strip() + '\n')
                for i in range(len(page_results) - 1, -1, -1):
                    file.write(page_results[i].__str__() + "\n")
            
                    
        # release uneeded XML from memory
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

        if article_count != -1 and article_count == 0:
            break

    del context
    
    
####### METADATA DOWNLOAD ENWIKI #######

#Download the zips from wiki metadata dump
def download_metadata_zips(num_files_download, outdir, overwrite=False):
    '''
    download enwiki data dumps, number defines how many
    '''
    import requests
    import urllib.request
    from bs4 import BeautifulSoup
    import os

    url = 'https://dumps.wikimedia.org/enwiki/20200401/'
    response = requests.get(url)

    soup = BeautifulSoup(response.text, "html.parser")
    ul_links = soup.findAll('ul')[3].findAll('a')

    if not os.path.exists(outdir):
        os.makedirs(outdir, exist_ok=True)

    for i in range(num_files_download): #'a' tags are for links
        print("Downloading enwiki file: (" + str(i+1) + "/" + str(num_files_download) + ")")

        link = ul_links[i]['href']
        download_url = 'https://dumps.wikimedia.org/enwiki/20200401/enwiki-20200401-stub-meta-history' + str(i+1) + '.xml.gz'
        print(download_url)
        filename = os.path.join(outdir, "metadata-20200401-" + str(i+1) + ".xml.gz") #generate filename

        #if file name exists then skip, and not overwrite
        if not overwrite and os.path.exists(filename):
            continue

        urllib.request.urlretrieve(download_url, filename) #pass filename to urllib request
            
def gunzip_shutil(filepath, outdir, block_size=65536):
    import gzip
    import shutil
    
    if not os.path.exists(outdir):
         os.mkdir(outdir)

    filename = outdir + "/" + filepath.split("/")[-1][:-3]
    
    #if file name exists then skip, and not overwrite
    if os.path.exists(filename):
        print("Extracted file already exists: {}".format(filename))
        return

    print("Extracting file: {} --- to path --- {}".format(filepath, outdir))
    with gzip.open(filepath, 'rb') as s_file, \
            open(filename, 'wb') as d_file:
        shutil.copyfileobj(s_file, d_file, block_size) 
        
class MetadataRevision:
    def __init__(self, timestamp, revert, version, contributor, revision_id, revision_parentid, sha1, text_bytes):
        self.timestamp = timestamp
        self.revert = revert
        self.version = version
        self.contributor = contributor
        self.revision_id = revision_id
        self.revision_parentid = revision_parentid
        self.sha1 = sha1
        self.text_bytes = text_bytes
        
    def __repr__(self):
        return [self.timestamp, self.revert, self.version, self.contributor, self.revision_id, self.revision_parentid]
        
    def __str__(self):
        try:
            return "^^^_" + self.timestamp + " " + str(self.revert) + " " + str(self.version) + " " + str(self.text_bytes) + " " + self.contributor
        except:
            print(self.timestamp)
            print(self.revert)
            print(self.version)
            print(self.contributor)
    
    def get_revision_id(self):
        return self.revision_id
        
#parse the file, calculate lightdump information, output to outfile
def parse_metadata_to_lightdump(filepath, outfile, outdir, articles=[]):

    from lxml import etree
    import re

    context = etree.iterparse(filepath, tag='{http://www.mediawiki.org/xml/export-0.10/}page', encoding='utf-8')
    nsmap = {'ns': 'http://www.mediawiki.org/xml/export-0.10/'}

    article_count = len(articles) if len(articles) != 0 else -1
    
    revi_header = "^^^_"

    page_dicts = {}
            
    if not os.path.exists(outdir):
        os.makedirs(outdir, exist_ok=True)
        
    if os.path.exists(outdir + "/" + outfile):
        print("Lightdump File Already Exists")
        return

    with open(outdir + "/" + outfile, 'w') as file:
        file.write("")
        
    with open(outdir + "/" + 'article_titles.txt', 'a') as article_file:
        article_file.write("")

    for event, elem in context:

        page_title = '_'.join(elem.find('ns:title', nsmap).text.split())
        page_id = elem.find('ns:id', nsmap).text
        revisions = elem.findall('ns:revision', nsmap)

        rev_dicts = []
        
        if len(articles) != 0 and page_title not in articles:
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            continue
        
#         print(page_title)
#         print(len(revisions))
        
        for revision in revisions:
            rev = {}
            
            rev['revision_id'] = revision.find("ns:id", nsmap).text
            rev['revision_parentid'] = revision.find("ns:parentid", nsmap).text if revision.find("ns:parentid", nsmap) != None else None
            rev['timestamp'] = revision.find("ns:timestamp", nsmap).text
            rev['sha1'] = revision.find("ns:sha1", nsmap).text
            rev['text_bytes'] = revision.find("ns:text", nsmap).attrib['bytes']
            
#             print(rev['text_bytes'])
             
            #getting contributer info
            contributor_info = revision.find("ns:contributor", nsmap)
            revision_contributor_username = contributor_info.find("ns:username", nsmap).text if contributor_info.find("ns:username", nsmap) != None else None
            revision_contributor_id = contributor_info.find("ns:id", nsmap).text if contributor_info.find("ns:id", nsmap) != None else None
            revision_contributor_ip = contributor_info.find("ns:ip", nsmap).text if contributor_info.find("ns:ip", nsmap) != None else None
            
            if revision_contributor_username != None:
                revision_contributor_username = "_".join(revision_contributor_username.split())
                rev['contributor'] = revision_contributor_username
            elif revision_contributor_ip != None:
                rev['contributor'] = revision_contributor_ip
            else:
                rev['contributor'] = "null"
                
            rev_dicts.append(rev)
            
        rev_dicts.sort(key=lambda x: x['timestamp'])

        version = 1
        page_results = []

        for i in range(len(rev_dicts)):

            temp_rev = MetadataRevision(rev_dicts[i]['timestamp'], 0, version, rev_dicts[i]['contributor'], \
                                rev_dicts[i]['revision_id'], rev_dicts[i]['revision_parentid'], \
                                rev_dicts[i]['sha1'], rev_dicts[i]['text_bytes'])
            if rev_dicts[i]['revision_parentid'] == None:
                page_results.append(temp_rev)
                version += 1    
            else:
                # find the point we revert to
                min_ind = len(page_results) - 1
                while min_ind > 0:
                    if page_results[min_ind].sha1 == rev_dicts[i]['sha1']:
                        temp_rev.version = page_results[min_ind].version
                        temp_rev.revert = 1
                        page_results.append(temp_rev)
                        break
                    min_ind -= 1

                if min_ind == 0:
                    page_results.append(temp_rev)
                    version += 1
        
        if len(articles) == 0 or page_title in articles:
            article_count -= 1
    
            print("Writing {} {} revisions to {}".format(page_title, len(page_results), outfile))
            with open(outdir + "/" + outfile, 'a') as file:
                with open(outdir + "/" + 'article_titles.txt', 'a') as article_file:
                    file.write(page_title.strip() + '\n')
                    article_file.write(page_title.strip() + "\n")
                    for i in range(len(page_results) - 1, -1, -1):
                        file.write(page_results[i].__str__() + "\n")
            
        # release uneeded XML from memory
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

        if article_count != -1 and article_count == 0:
            break

    del context
    
    