#!/usr/bin/python

import tweepy, webbrowser, threading, traceback, string, rake_nltk, nltk, re, time
from unidecode import unidecode
from datetime import datetime
from wordcloud import WordCloud
from cassandra.cluster import Cluster

# Static Global Variables
CONSUMER_KEY        = "AAA"
CONSUMER_SECRET     = "BBB"
ACCESS_TOKEN        = "CCC"
ACCESS_TOKEN_SECRET = "DDD"

# Authenticating
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True)

class cassandra:
    def __init__(self):
        self.myDict = {}
        self.session = Cluster().connect()
        self.query =  '''
            INSERT INTO twitter.ss (
                  time_partition
                , status_id
                , status_created_at
                , in_reply_to_status_id
                , in_reply_to_user_id
                , user_id
                , user_created_at
                , user_statuses_count
                , user_favourites_count
                , user_followers_count
                , user_friends_count
                , in_reply_to_screen_name
                , user_screen_name
                , clean_text
                , status_text
                , rake_list
                , noun
                , preposition
                , verb
                , clause
            ) VALUES (
                  %(time_partition)s
                , %(status_id)s
                , %(status_created_at)s
                , %(in_reply_to_status_id)s
                , %(in_reply_to_user_id)s
                , %(user_id)s
                , %(user_created_at)s
                , %(user_statuses_count)s
                , %(user_favourites_count)s
                , %(user_followers_count)s
                , %(user_friends_count)s
                , %(in_reply_to_screen_name)s
                , %(user_screen_name)s
                , %(clean_text)s
                , %(status_text)s
                , %(rake_list)s
                , %(noun)s
                , %(preposition)s
                , %(verb)s
                , %(clause)s
            );
        '''
        # sceen_names and ids
        self.idDict = {
              'thehill'         :   '1917731'
            , 'guardian'        :   '87818409'
            , 'TheEconomist'    :   '5988062'
            , 'heine_gregor'    :   '932237227354132480'
            , 'MSNBC'           :   '2836421'
            , 'HuffPost'        :   '14511951'
        }
    # disconnect
    def shutdown(self):
        self.session.shutdown()
    # insert
    def insert(self, insertDict):
        self.session.execute(self.query, insertDict)
# Instantiation
myDB = cassandra()

class workText:
    def __init__(self):
        self.rakeObj = rake_nltk.Rake()
        self.rakeDict = {}
        self.ranking = None
        self.wordList = None
        self.punct = set(string.punctuation)
        #self.snowStemmer = nltk.stem.snowball.SnowballStemmer("english")
        self.stopwords = set(nltk.corpus.stopwords.words('english'))
        self.chunkParser = nltk.RegexpParser(
            r"""
                NP: {<DT|JJ|NN.*>+}          # Chunk sequences of DT, JJ, NN
                PP: {<IN><NP>}               # Chunk prepositions followed by NP
                VP: {<VB.*><NP|PP|CLAUSE>+$} # Chunk verbs and their arguments
                CLAUSE: {<NP><VP>}           # Chunk NP, VP
            """)
    # Create clean text
    def clean(self, status_text):
        self.cleanText = unidecode(status_text)
        # regex substitute remove
        subList = [r"\n", r"^RT\s*", r"http\S*", r"htt\.*",
                r"#\w+", r"^\s*:*\s", r"\s+$",
                r"@\w+[:-;,.\s]?[:-;,.\s]?", r"\s*\|\s*\w*\s*\w*\s*"]
        for item in subList:
            self.cleanText = re.sub(item, '', self.cleanText, count=99)
        # string replace
        replaceList = [ ('  ', ' '), ('::', ':'), (',,', ','),
            ('...','.'), ('&amp;','and'), ("'ll", ' will'),
            ("'ve", ' have'), ("'re", ' are'), ("'m", ' am')]
        for x, y in replaceList:
            self.cleanText = self.cleanText.replace(x, y)
    # Tokenization & Snowball Stemming
    def snowball(self, clean_text=None):
        self.nounList = []
        self.prepositionList = []
        self.verbList = []
        self.clauseList = []
        if clean_text == None:
            clean_text = self.cleanText
        self.snowToken = nltk.tokenize.casual_tokenize(clean_text) # next: rm stopwords
        self.snowToken = [x for x in self.snowToken
            if not x.lower() in list(self.stopwords)+list(self.punct)]
        self.snowPosTag = nltk.pos_tag(self.snowToken)
        self.snowTree = self.chunkParser.parse(self.snowPosTag)
        for node in self.snowTree.subtrees():
            if node.label() == 'NP':
                self.nounList += [unidecode(word) for (word, pos) in node.leaves()]
            if node.label() == 'PP':
                self.prepositionList += [unidecode(word) for (word, pos) in node.leaves()]
            if node.label() == 'VP':
                self.verbList += [unidecode(word) for (word, pos) in node.leaves()]
            if node.label() == 'CLAUSE':
                self.clauseList += [unidecode(word) for (word, pos) in node.leaves()]
    ## Lemmatization
    # def lemmatize(self, clean_text=None):
    #     self.lemmBigram = []
    #     if clean_text == None:
    #         clean_text = self.cleanText
    #     tokenList = nltk.tokenize.casual_tokenize(clean_text)
    #     tokenList = [x for x in tokenList if not x.lower()
    #        in set(nltk.corpus.stopwords.words('english'))]
    #     self.lemmToken = [unidecode(nltk.stem.WordNetLemmatizer().lemmatize(x))
    #        for x in tokenList if x not in set(string.punctuation)]
    #     self.lemmTkFreq = nltk.FreqDist(self.lemmToken)
    #     self.lemmPosTag = [nltk.pos_tag(nltk.word_tokenize(x)) for x in self.lemmToken]
    # Rapid Automatic Keyword Extraction (RAKE)
    def rakeDo(self, clean_text=None):
        self.rakeList = []
        if clean_text == None:
            clean_text = self.cleanText
        self.rakeObj.extract_keywords_from_text(clean_text)
        # WordDictCount
        for item in self.rakeObj.get_ranked_phrases():
            self.rakeList.append(item)
            if item in self.rakeDict.keys():
                self.rakeDict[item] += 1
            else:
                self.rakeDict[item] = 1

# Ranking
def rankTop(inDict, topN=12):
    return sorted(zip(inDict.values(), inDict.keys()), reverse=True)[:topN]
# Type enforcement
def stringEnforce(var):
    if type(var) == str:
        return var
    elif type(var) == unicode:
        return unidecode(var)
    elif type(var) == int:
        return var
    elif type(var) == list:
        if len(var) == 0:
            return None
        elif len(var) == 1:
            if type(var[0]) == int:
                return var[0]
            elif type(var[0]) == unicode:
                return unidecode(var[0])
            elif type(var[0]) == str:
                return var[0]
        elif len(var) > 1:
            return unidecode(var[0])
        else:
            return 'list len > 1'
    elif var == [] or var == [()] or var == [''] or var == None:
        return None
    else:
        return 'unknown'

class myListener(tweepy.streaming.StreamListener):
    def on_status(self, status):
        global myText, myDB
        try:
            # Threading relaxe concorrency constraint of single thread GIL
            threading.Thread(name='myText.clean', target=myText.clean(status.text)).start()
            threading.Thread(name='myText.snowball', target=myText.snowball()).start()
            threading.Thread(name='myText.rakeDo', target=myText.rakeDo()).start()
            print '\n'+myText.cleanText
            print myText.nounList
            print myText.rakeList
            # Create temporary rowDict object                      #PARTITION_SECONDS
            time_partition = int(status.created_at.strftime('%s')) /60/5*60*5
            rowDict = {
                  'time_partition'          : time_partition
                , 'status_id'               : status.id
                , 'status_created_at'       : int(status.created_at.strftime('%s'))
                , 'in_reply_to_status_id'   : status.in_reply_to_status_id
                , 'in_reply_to_user_id'     : status.in_reply_to_user_id
                , 'user_id'                 : status.user.id
                , 'user_created_at'         : int(status.user.created_at.strftime('%s'))
                , 'user_statuses_count'     : status.user.statuses_count
                , 'user_favourites_count'   : status.user.favourites_count
                , 'user_followers_count'    : status.user.followers_count
                , 'user_friends_count'      : status.user.friends_count
                , 'in_reply_to_screen_name' : stringEnforce(status.in_reply_to_screen_name)
                , 'user_screen_name'        : stringEnforce(status.user.screen_name)
                , 'clean_text'              : myText.cleanText
                , 'status_text'             : unidecode(status.text)
                , 'rake_list'               : myText.rakeList
                , 'noun'                    : myText.nounList
                , 'preposition'             : myText.prepositionList
                , 'verb'                    : myText.verbList
                , 'clause'                  : myText.clauseList
            }
            # Writing to cassandra
            myDB.myDict = rowDict
            threading.Thread(name='myDB.insert', target=myDB.insert(rowDict)).start()
        except:
            myStream.disconnect()
            traceback.print_exc()

 # global collector names
myDB = cassandra()
myText = workText()

# Initiating stream
myStream = tweepy.Stream(auth=api.auth, listener=myListener())
myStream.filter(follow=myDB.idDict.values(), async=True)

# Rank in memory Python results
rankTop(myText.rakeDict)
rankTop(myText.nodeDict)

# Disconnecting
myStream.disconnect()
myDB.shutdown()