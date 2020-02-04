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



# def ucwords(string):
# 	return string[0].upper()+string[1:]


# import re
# def ucwords (s):
#     """Returns a string with the first character of each word in str 
#     capitalized, if that character is alphabetic."""
#     return " ".join([w[0].upper() + w[1:] for w in re.split('\s*', s)])

	
@route('/')
def home():
		return 'radio gaga'


@route('/findall')
def findall():
		# aws = AWS4Auth(AWS_ACCESS_ID, AWS_SECRET_KEY, AWS_DEFAULT_REGION, 'es')
		# es = Elasticsearch(
		# 		hosts=ES_HOST,
		# 		http_auth=aws,
		# 		use_ssl=False,
		# 		verify_certs=True,
		# 		connection_class=RequestsHttpConnection)
		
		query = {
			"query": {
				"match": {"task.platform": "twitter"}
			}
		}
		es = Elasticsearch(
		# http_auth= ('elastic','changeme'),
		port= 9200,
		hosts='localhost'
		)
		result = es.search(index='feeds', body=query)
		return result

@route('/find')
# @route('/find/<my_last:int>')
def find():
		
		es = Elasticsearch(
		# http_auth= ('elastic','changeme'),
		port= 9200,
		hosts='localhost'
		)
		
		end = int(time.time())
		awal = int(time.time())
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
			# doc_type='feeds',
			request_timeout=2000
			)

		# print('start: '+ str(start), ' end: '+str(end))
		
		sourcelabel = {}
		tot = 0
		for label in result:
			# print('a')
			tot += 1
			try:
				# print(result)
				key = (label['_source']['metadata']['sourcelabel'].lower()).title()
				# sourcelabel.append(label['_source']['metadata']['sourcelabel'].lower())
				try:
					sourcelabel[key] += 1
				except:
					sourcelabel[key] = 1
			except Exception as err:
				print(err)
		# print(sourcelabel)

		# balikan = Counter(sourcelabel).most_common()
		# uc = [(ucfirst(k), v) for k,v in sourcelabel]

		balikan = sorted(sourcelabel.items(), key=lambda kv: kv[1], reverse=True)
		akhir = time.time()
		selisih = akhir - awal
		print(selisih)
		print(tot)
		return dict(balikan)


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

@route('/listing')
# @route('/listing/<my_last:int>')
def listing():
	es = Elasticsearch(
		# http_auth= ('elastic','changeme'),
		port= 9200,
		hosts='localhost'
		)

	hasil = {}
	awal = time.time()
	for client in list_client:
		# print(client)
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
				size=14
			)
			print(result['hits']['total'])
			value = result['hits']['total']
			hasil[client['key']] = value
			
		except:
			print('salah source ni')

	balikan = sorted(hasil.items(), key=lambda kv: kv[1], reverse=True)
	end = time.time()
	print('speed: '+ str(end-awal))

	# return {'response':result}
	return dict(balikan)



if __name__ == "__main__":
		run(port=7777, debug=True)
