#!/usr/bin/python

############
# LISTENER #
############
import tweepy

# 1) Create class to inherit from tweepy StreamListener
class MY_LISTENER(tweepy.StreamListener):
    def on_status(self, status):
    print status.text

# 2) Create stream object
MY_LISTENER = MY_LISTENER()
MY_STREAM = tweepy.Stream(auth = api.auth, listener=MY_LISTENER())

# 3) Connecting to the Twitter streaming API
MY_STREAM.filter(track=['keyword','list'])


##################
# DATA RETRIEVAL #
##################

import nltk
from cassandra.cluster import Cluster

session = Cluster().connect()

query = '''
    SELECT IN_REPLY_TO_STATUS_ID FROM twitter.tt
    WHERE IN_REPLY_TO_STATUS_ID > 0 ALLOW FILTERING;
'''

# SELECT COUNT(*), id GROUP BY id ORDER BY COUNT(*) DESC; equivalent
rowList = list(session.execute(query))
itemList = [row[0] for row in rowList]
freqDict = nltk.FreqDist(itemList)
freqDict.most_common(12)



################################
# RETRIEVE - TRAVERSE - RETURN #
################################

import nltk
from cassandra.cluster import Cluster
from scipy.misc import imsave
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# Headline >> Senate GOP: We will grow our majority in midterms
session = Cluster().connect()
query = '''
    SELECT USER_FOLLOWERS_COUNT, NOUN FROM twitter.tt
    WHERE IN_REPLY_TO_STATUS_ID = %s ALLOW FILTERING;
''' %(974246607607779328)

rowList = list(session.execute(query))

# List traversal and dictionary compilation
snow = nltk.stem.snowball.SnowballStemmer("english")

pDict = {}
cDict = {}
stemList = []

for row in rowList:
    if row[1] is not None:
        wStr = str()
        for word in row[1]:
            wStr += word+' '; stemList.append(snow.stem(word.lower()))
        pDict[wStr[:-1]] = row[0]; cDict[wStr[:-1][:16]] = row[0]

print sorted(zip(pDict.values(), pDict.keys()), reverse=True)[:6]
print nltk.FreqDist(stemList).most_common(6)

# Visualize with word cloud
WC = WordCloud(width=600,height=300,background_color='white',max_words=32)
WC.generate_from_frequencies(cDict) # WC.generate("+".join(stemList))
plt.imshow(WC, interpolation='nearest', aspect='equal')
imsave('/directory/path/wcImage.png', WC)
