"""
Python script to poll Twitter API and store mentions in a CouchDB instance.
"""

import config
import couchdb
import itertools
import json
import sys
import time
from urllib2 import urlopen, HTTPError
from urllib import urlencode


# Create new connection to CouchDB instance.
couch = couchdb.Server(
    'http://{userid}:{password}@{host}'.format(**config.couchdb))
db = couch[config.couchdb['dbname']]

def search_tweets(rpp=100, page=1, **params):
    params['rpp'] = rpp
    params['page'] = page

    while True:
        try:
            print 'Retrieving page {page}'.format(**params)

            tweet_data = urlopen('http://search.twitter.com/search.json?{0}'
                                 .format(urlencode(params))).read()
            tweets = json.loads(tweet_data)['results']

            for tweet in tweets:
                yield tweet

            params['page'] += 1

        except HTTPError:
            break

def prepare_tweet(tweet):
    print 'Preparing tweet ' + unicode(tweet)

    doc = tweet.copy()
    doc['_id'] = str(tweet['id'])

    return doc

def save_tweets(tweets):
    print 'Saving tweets...' # + unicode(tweet)
    try:
        db.update(tweets)
    except Exception, e:
        print 'Could not save tweets: ' + unicode(e)

    print

# At a set interval, fetch all mentions
def fetch_tweets():
    print db

    docs = db.view('_all_docs', descending=True, limit=1, skip=1)
    if len(docs) == 0:
        since_id = 10000
    else:
        doc = docs.rows[0]
        since_id = doc.id

    tweet_sets = []
    for keyword in config.search_keywords:
        tweet_set = search_tweets(q=keyword, since_id=since_id)
        tweet_sets.append(tweet_set)

    tweet_docs = []
    for tweet in itertools.chain(*tweet_sets):

        # Check if tweet fetched matches since_id (Twitter API bug?)
        if tweet['id'] == since_id:
            continue

        # Skip non geo-tagged tweets
        if 'geo' not in tweet or tweet['geo'] is None:
            continue

        tweet_docs.append(prepare_tweet(tweet))
    save_tweets(tweet_docs)


if __name__ == '__main__':
    while True:
        fetch_tweets()
        time.sleep(config.timers['interval']/1000.0)
    sys.exit(main())
