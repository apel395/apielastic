from elasticsearch import Elasticsearch
#from elasticsearch import RequestsHttpConnection
from elasticsearch.helpers import scan
from bottle import route, run
#from config import AWS_ACCESS_ID
#from config import AWS_DEFAULT_REGION
#from config import AWS_SECRET_KEY
#from config import ES_HOST
#from config import ES_INDEX
#from requests_aws4auth import AWS4Auth
from collections import Counter
import time
import datetime
import json
import string



list_client = [
				{"key": "Twitter for Android"},
				{"key": "Twitter for iPhone"},
				{"key": "Twitter Web App"},
				{"key": "Echobox Social"},
				{"key": "twittbot.net"},
				{"key": "TweetDeck"},
				{"key": "DopeyUncle2"},
				{"key": "Instagram"},
				{"key": "TweetCaster for Android"},
				{"key": "Mobile Web (M2)"},
				{"key": "Twitter for iPad"},
				{"key": "Cheap Bots, Done Quick!"},
				{"key": "Twitter Web Client"},
				{"key": "Tabtter Free"},
				{"key": "WordPress.com"},
				{"key": "Facebook"},
				{"key": "dlvr.it"},
				{"key": "IFTTT"},
				{"key": "BERITAKINI.CO"},
				{"key": "Buffer"},
				{"key": "Echofon"},
				{"key": "Find Lat Lng"},
				{"key": "Foursquare"},
				{"key": "Just For Your Information"},
				{"key": "TwitCasting"},
				{"key": "Ask.fm"},
				{"key": "CoSchedule"},
				{"key": "Flexi Recipes"},
				{"key": "Foursquare Swarm"},
				{"key": "Google"},
				{"key": "Hootsuite Inc."},
				{"key": "LaterMedia"},
				{"key": "TheJakartaGlobe"},
				{"key": "Trubus Indonesia"},
				{"key": "Tweet Old Post"},
				{"key": "Twitter Media Studio"},
				{"key": "Twitter for Mac"},
				{"key": "UberSocial for Android"},
				{"key": "detikcommunity"},
				{"key": "lsisi.id"}
			]

	
@route('/')
def home():
		return 'radio gaga'


@route('/findall')
def findall():
		
		query = {
			"query": {
				"match": {"task.platform": "twitter"}
			}
		}
		es = Elasticsearch(
		port= 9200,
		hosts='localhost'
		)
		result = es.search(index='feeds', body=query)
		return result

@route('/find')
def find():
		
		es = Elasticsearch(
		port= 9200,
		hosts='localhost'
		)
		
		end = int(time.time())
		query={
			"query": {
				"bool": {
					"must": [{
						"match": {
							"task.platform": "twitter"}
							},{
						"range": {
							"createdat": {
								"gte": 0,
								"lte": end
							}
						}
					}]
				}
			}
		}
		result = scan(
			es, 
			index='feeds', 
			query=query,
			request_timeout=2000
			)
		
		sourcelabel = {}
		tot = 0
		for label in result:
			tot += 1
			try:
				key = (label['_source']['metadata']['sourcelabel'].lower()).title()
				try:
					sourcelabel[key] += 1
				except:
					sourcelabel[key] = 1
			except Exception as err:
				print(err)

		balikan = sorted(sourcelabel.items(), key=lambda kv: kv[1], reverse=True)
		akhir = time.time()
		selisih = akhir - end
		print(selisih)
		print(tot)
		return dict(balikan)


@route('/listing')
def listing():
	es = Elasticsearch(
		port= 9200,
		hosts='localhost'
		)

	hasil = {}
	awal = time.time()
	for client in list_client:

		try:
			query={
				"query": {
					"bool": {
						"must": [{
							"match_phrase": {
								"metadata.sourcelabel":  '{}'.format(client['key'])}
							}]
						}
					}
				}
			print(query)
			result = es.search(
				index='feeds',
				body=query,
				size=0
			)
			print(result['hits']['total'])
			value = result['hits']['total']
			hasil[client['key']] = value
			
		except:
			print('salah source ni')

	balikan = sorted(hasil.items(), key=lambda kv: kv[1], reverse=True)
	end = time.time()
	print('speed: '+ str(end-awal))

	return dict(balikan)



if __name__ == "__main__":
		run(port=7777, debug=True)
