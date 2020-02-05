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
from collections import Counter, OrderedDict
import time
import datetime
import json
import string


	
es = Elasticsearch(
	port= 9200,
	hosts='localhost'
	)
esIndex = 'feeds'


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

	result = es.search(index=esIndex, body=query)
	return result


@route('/find')
def find():
		
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
		index=esIndex, 
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
	
	hasil = {}
	awal = time.time()
	from listClient import client_list
	
	for client in client_list:

		try:
			query={
				"query": {
					"bool": {
						"must": [{
							"match_phrase": {
								"metadata.sourcelabel":  client}
							}]
						}
					}
				}
			print(query)
			result = es.search(
				index=esIndex,
				body=query,
				size=0
			)
			hasil[client] = result['hits']['total']
			
		except:
			print('salah source ni')

	balikan = OrderedDict(sorted(hasil.items(), key=lambda kv: kv[1], reverse=True))
	# print(balikan)
	end = time.time()
	
	print('speed: '+ str(end-awal))
	print(len(balikan))
	print(sum(hasil.values()))

	return balikan



if __name__ == "__main__":
	run(port=7777, debug=True)
