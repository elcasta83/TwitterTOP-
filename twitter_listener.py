#!/usr/bin/python
 # -*- coding: utf-8 -*- 

import socket
import sys
import requests
import requests_oauthlib
import json
import configTwitter
#Variables para API TWITTER
ACCESS_TOKEN = configTwitter.ACCESS_TOKEN
ACCESS_SECRET = configTwitter.ACCESS_SECRET
CONSUMER_KEY = configTwitter.CONSUMER_KEY
CONSUMER_SECRET = configTwitter.CONSUMER_SECRET
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
        	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
        	query_data = [('language', 'es'),('locations', '-130,-20,100,50'),('track','#')]
        	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
        	response = requests.get(query_url, auth=my_auth, stream=True)
        	print(query_url, response)
        	return response

def send_tweets_to_spark(http_resp, tcp_connection):
   for line in http_resp.iter_lines():
      	try:
        	full_tweet = json.loads(line)
        	tweet_text = full_tweet['text']
        	print ("--------------abre tuit----------------------------")
        	print("Tweet Text: " + tweet_text)
        	tcp_connection.sendall(tweet_text.encode('utf-8'))
      	except: 
      		print("Error: %s" % sys.exc_info())

TCP_IP = "localhost"
TCP_PORT = 9990
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")

resp = get_tweets()
send_tweets_to_spark(resp, conn)
