from datetime import datetime
import pandas as pd
import seaborn as sns
import matplotlib.pylab as plt
import pandas.util.testing as tm

def calc_m_score(user_edits, revert_pairs, mutual_revert_pairs, mutual_revert_users):
    score = 0
    max_edits = 0
    
    for pair in revert_pairs:
        parts = pair.split("~!~")
        u1 = parts[0]
        u2 = parts[1]
        if user_edits[u1] < user_edits[u2]:
            edit_min = user_edits[u1]
        else:
            edit_min = user_edits[u2]
        
        if ((edit_min > max_edits) and 
            (pair in mutual_revert_pairs 
             or u2 + "~!~" + u1 in mutual_revert_pairs)):
            max_edits = edit_min
            
        score += edit_min
        
    score -= max_edits

    score *= len(mutual_revert_users)
    return score

def get_mutual(revert_pairs):
    mutual_revert_pairs = []
    mutual_revert_users = []

    for pair in revert_pairs:
        parts = pair.split("~!~")
        
        if parts[1] + "~!~" + parts[0] in revert_pairs:
            sorted_pair = ""
            if parts[0] < parts[1]:
                sorted_pair = parts[0] + "~!~" + parts[1]
            else:
                sorted_pair = parts[1] + "~!~" + parts[0]
                
            mutual_revert_pairs.append(sorted_pair)
            
            if parts[1] not in mutual_revert_users:
                mutual_revert_users.append(parts[1])
            if parts[0] not in mutual_revert_users:
                mutual_revert_users.append(parts[0])
                
    return (mutual_revert_pairs, mutual_revert_users)

def process_article(article_edits):
    user_edits = {}
    num_reverts = 0
    revert_pairs = []
    mscore = 0
    mutual_reverts = []
    
    #^^^_2010-02-02T07:20:39Z 0 -1 Jijo_mt
    for i in range(len(article_edits)):
#         print(article_edits[i])
        if len(article_edits[i]) <= 1: #title article
            continue
        try:
            if article_edits[i][3] in user_edits:
                user_edits[article_edits[i][3]] += 1
            else:
                user_edits[article_edits[i][3]] = 1
        except:
            print("Couldnt process: " + str(article_edits[i]))
            raise
            
        if article_edits[i][1] == '1': #if this is a reverting edit
            num_reverts += 1
            
            #get the line of the revert
            temp_revert_index = -1
            for revert_index in range(i + 1, len(article_edits)):
                
                if article_edits[revert_index][2] == article_edits[i][2]:
                    temp_revert_index = revert_index
                    break #break when matching revert index found
                    
            if article_edits[temp_revert_index][3] == article_edits[i][3]: #if same author ignore
                continue
                
            if temp_revert_index == i + 1: #if revert between the editor of revision j and i respectively, ignore
                continue
            else:
                pair = article_edits[revert_index][3] + "~!~" + article_edits[i][3]
                revert_pairs.append(pair)
                
    mutual_reverts = get_mutual(revert_pairs)
    mscore = calc_m_score(user_edits, revert_pairs, mutual_reverts[0], mutual_reverts[1])
   
    return [num_reverts, len(mutual_reverts), mscore]

def parse_lightdump_mscore(filepath):  
    mscores = {}
    article_edits = []
    num_articles = 0
    current_title = ""
    start = False
    
    with open(filepath) as file:
        for line in file:
            temp_line = line.strip().split(" ")
                
            if len(temp_line) == 1:
                start = True
                current_title = temp_line[0]
                article_edits.append(temp_line)
            #when we get new article we are done
            elif len(temp_line) == 4 and temp_line[2] == "1" and start:
                start = False
                mscores[current_title] = process_article(article_edits)[2]
                num_articles += 1
                article_edits = []
                
            #as long as start is true we are still adding from our article
            else:
                article_edits.append(temp_line)
           
    return mscores
    
def mscore_over_time(filepath, article_title, outfile):
    article_edits = []
    current_title = ""
    start = False
    mscores = []
    times = []
    
    with open(filepath) as file:
        for line in file:
            temp_line = line.strip().split(" ")
            
            #if it finds the article start appending edits
            if len(temp_line) == 1 and temp_line[0] == article_title:
                start = True
                article_edits.append(temp_line)
            #when we get new article we are done
            elif len(temp_line) == 4 and temp_line[2] == "1" and start:
                start = False
                break
                
            #as long as start is true we are still adding from our article
            elif start:
                article_edits.append(temp_line)
                
    while len(article_edits) > 5:
        times.append(article_edits[-1][0][4:])
        mscores.append(process_article(article_edits)[2])
        article_edits.pop()
        article_edits.pop()
        article_edits.pop()
        article_edits.pop()
        article_edits.pop()
        

    datetimes = [datetime.strptime(tm, '%Y-%m-%dT%H:%M:%SZ') for tm in times]
    mscores = mscores[::-1]

    df = pd.DataFrame({'time': datetimes, 'mscore': mscores})

    plot = sns.lineplot(x='time', y='mscore', data=df)
    plot.set_title(article_title)
    plt.xlabel("time")
    plt.ylabel("M-Score")
    
    plt.savefig(outfile + '.png')
    
    plt.close()
