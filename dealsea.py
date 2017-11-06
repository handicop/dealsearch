#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib2
from bs4 import BeautifulSoup
import re
from sqlalchemy import create_engine
from sqlalchemy import exc
# from elasticsearch import Elasticsearch
import datetime
import logging
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class Dealsea:

	es = None
	engine = None
	urlstr = None

	def __init__(self):
		logging.info('Dealsea initialized')
		config = {}
		try:
			with open('/home/pi/Documents/DealProject/config.json', 'r') as f:
				config = json.load(f)
		except Exception as e:
			logging.exception(e)
		self.headers = {'User-Agent':'Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_2_1 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8G4 Safari/6533.18.5'}
		self.engine = create_engine(config['mysql'])
		self.urlstr = config['dealsea_url']
		# try:
		# 	self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
		# except Exception, e:
		# 	logging.error(e)
		# 	self.es = None

	def run(self):		
		req = None
		try:
			req = urllib2.Request(self.urlstr, None, self.headers)
			htmlSource = urllib2.urlopen(req).read()
		except Exception as e:
			logging.exception(e)
		
		soup = BeautifulSoup(htmlSource, "html5lib")
		dealText = soup.findAll("a", href=re.compile('^/view-deal/'))

		# deal count is less than 1 then quit
		if len(dealText) < 1:
			logging.warn('deal qty is less than 1')
			return

		logging.info('deal qty %d' % len(dealText))
		con = None
		try:
			con = self.engine.connect()

			insertQuery = """insert into deal_history (`url`, `content`, `img`, `site`) values (%s, %s, %s, 'dealsea.com')"""

			with con:
				
				for m in dealText:
					try:
						eachOne = BeautifulSoup(str(m), "html5lib")
						content = eachOne.find("p", {"class":"t"}).contents[0]
						url = eachOne.find("a", href=re.compile('^/view-deal/'))['href']
						img = eachOne.find("img", src=re.compile('^https://'))['src']
						try:
							con.execute(insertQuery, ('http://www.dealsea.com'+url, content, img))
							# if self.es != None:
							# 	try:
							# 		doc = {
							# 			'site':'dealsea.com',
							# 			'url':'http://www.dealsea.com'+url,
							# 			'content':content,
							# 			'reg_dtime':str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
							# 		}
							# 		res = self.es.index(index="deals", doc_type='deal', body=doc)
							# 		logging.debug(res)
							# 	except Exception as e:
							# 		logging.exception(e)
						except exc.SQLAlchemyError as se:
							logging.debug('duplicated record')
						except Exception as e:
							logging.exception(e)
					except (NameError, TypeError, AttributeError):
						logging.debug('not found')
					except Exception as e:
						logging.exception(e)
		except Exception as e:
			logging.exception(e)
		finally:
			if con != None:
				con.close()


