#!/usr/bin/env python
import pycurl, json, urllib, boto, setting
from multiprocessing import Queue, Process, cpu_count
from query import SDBQuery
from keywords import get_filter_keywords


class TweetManager:
      def __init__(self):
            self.sdb = boto.connect_sdb(setting.AWS_KEY, setting.AWS_SECRET)
            self.__keywords__ = get_filter_keywords(self.sdb)
            self.__cores__ = cpu_count()
            self.tweets_queue = Queue()
            self.db_tweets = self.sdb.get_domain(setting.SDB_DOMAIN)
            self.__buffer__ = ""
      
      def connect_twitter(self):
            self.conn = pycurl.Curl()
            self.conn.setopt(pycurl.POSTFIELDS,urllib.urlencode(self.__keywords__))
            self.conn.setopt(pycurl.USERPWD, "%s:%s" % (setting.TWITTER_ID, setting.TWITTER_PASSWORD))
            self.conn.setopt(pycurl.URL, setting.JSON_STREAMING_URI)
            print 'starting tweet_producer process'
            self.conn.setopt(pycurl.WRITEFUNCTION, lambda data: self.tweet_producer(data))

      
      def tweet_producer(self, tweet):
            self.__buffer__ += tweet
            if tweet.endswith("\r\n") and self.__buffer__.strip():
                  self.tweets_queue.put(self.__buffer__)
                  self.__buffer__ = ""

      def start(self):
            self.connect_twitter()
            print 'starting %d tweet_consumer process(s)' % self.__cores__
            self.consumers = [Process(target=tweet_consumer, args=(i, self.tweets_queue, self.db_tweets,))
                              for i in xrange(self.__cores__)]
            for c in self.consumers:
                  c.start()
            self.conn.perform()

 
def tweet_consumer(id, queue, db_tweets):
      while 1:
            tweet = queue.get()
            if tweet is None:
                  break
            tweet = ''.join(tweet)            
            tweet = json.loads(tweet)
            query = SDBQuery(tweet).form_query()
            item = db_tweets.new_item(tweet['id'])
            for k,v in query.items():
                  item[k] = v
            item.save()
      queue.put(None)

if __name__ == "__main__":
      manager = TweetManager()
      manager.start()
      
