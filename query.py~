from hashlib import md5
from re import sub

class SDBQuery:
    def __init__(self, tweet):
        self.tweet = tweet

    def form_query(self):
        '''replaces all occurrences of the regex in the string
        '''
        self.tweet['text'] = sub('RT @\S+ ', '', self.tweet['text'])
        self.hash_value = md5(self.tweet['text'].encode('UTF-8')).hexdigest()
        
        query = {'id' : int(self.tweet['id']), \
                     'user_id' : int(self.tweet['user']['id']), \
                     'user_name' : self.tweet['user']['name'], \
                     'user_location' : self.tweet['user']['location'], \
                     'in_reply_to_status_id_str' : self.tweet['in_reply_to_status_id_str'], \
                     'text' : self.tweet['text'], \
                     'lang' : self.tweet['user']['lang'], \
                     'created_at' : self.tweet['created_at'], \
                     'profile_image_url' : self.tweet['user']['profile_image_url'], \
                     'source' : self.tweet['source'], \
                     'geo' : self.tweet['geo'], \
                     'retweeted' : self.tweet['retweeted'], \
                     'in_reply_to_user_id' : self.tweet['in_reply_to_user_id'], \
                     'filtered' : str('false'), \
                     'text_hash' : self.hash_value }
        #print query
        return query
