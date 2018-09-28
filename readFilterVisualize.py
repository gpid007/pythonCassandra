import nltk
from cassandra.cluster import Cluster
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# Connect to local cluster & execute query
session = Cluster().connect()
queryAll = '''
    SELECT IN_REPLY_TO_STATUS_ID FROM twitter.ss
    WHERE IN_REPLY_TO_STATUS_ID > 0 ALLOW FILTERING;
'''
rowList = list(session.execute(queryAll))

# Find status_id with most replies in descending order
itemList = [row[0] for row in rowList]
freqDict = nltk.FreqDist(itemList)
subList = [x[0] for x in freqDict.most_common(24)]

# Print headline to console in order to choose one
for item in subList:
    query = '''
        SELECT STATUS_ID, clean_text FROM twitter.ss
        WHERE STATUS_ID = %s ALLOW FILTERING;
    ''' %item
    print list(session.execute(query))

# Choose one (maximum response) headline for further analysis
queryOne = '''
    SELECT USER_FOLLOWERS_COUNT, NOUN FROM twitter.ss WHERE
    IN_REPLY_TO_STATUS_ID = %s ALLOW FILTERING;
''' %subList[0]
rowList = list(session.execute(queryOne))

# Load SnowballStemmer and create stubs
snow = nltk.stem.snowball.SnowballStemmer("english")
wDict = {}; pDict = {}; cDict = {}; stemList = []

# List traversal and dictionary compilation
for row in rowList:
    if row[1] is not None:
        wStr = str()
        for word in row[1]:
            wStr += word+' '
            stemList.append(snow.stem(word.lower()))
        pDict[wStr[:-1]] = row[0]
        cDict[wStr[:-1][:16]] = row[0]

# Print top 12 results & generate word cloud object
print sorted(zip(pDict.values(), pDict.keys()), reverse=True)[:12]
print nltk.FreqDist(stemList).most_common(12)
wordCloud = WordCloud(width=800, height=400, background_color='white')
wordCloud.generate_from_frequencies(pDict)
# wordCloud.generate("+".join(stemList))

# Show word cloud visualization
plt.imshow(wordCloud)
plt.axis('off')
plt.show()