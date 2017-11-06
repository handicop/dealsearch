#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib2
from bs4 import BeautifulSoup
import re
from sqlalchemy import create_engine
from sqlalchemy import exc
# import MySQLdb as mdb
import datetime
# from elasticsearch import Elasticsearch
import logging
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class Slickdeals:

	#es = None
	engine = None
	urlstr = None

	def __init__(self):
		logging.info('Slickdeals.net initialized')
		config = {}
		
		try:
			with open('/home/pi/Documents/DealProject/config.json', 'r') as f:
				config = json.load(f)
		except Exception as e:
			logging.exception(e)

		self.engine = create_engine(config['mysql'])
		self.urlstr = config['slickdeals_url']

		self.headers = {'User-Agent':'Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_2_1 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8G4 Safari/6533.18.5'}
		# try:
		# 	self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
		# except Exception, e:
		# 	logging.error(e)
		# 	self.es = None

	def contentFilter(self, content):
		try:
			return content[0:content.index('-')]
		except Exception as e:
			logging.exception(e)
			return content

	def run(self):
		req = None
		htmlSource = None

		try:
			req = urllib2.Request(self.urlstr, None, self.headers)
			htmlSource = urllib2.urlopen(req).read()
		except Exception as e:
			logging.exception(e)
			return

		soup = BeautifulSoup(htmlSource, "html5lib")
		dealText = soup.findAll("div", {"class":"box  frontpage cat onFrontPage"})
		
		logging.info('deal qty %d' % len(dealText))
		
		con = None
		try:
			con = self.engine.connect()

			insertQuery = """insert into deal_history (`url`, `content`, `img`, `site`) values (%s, %s, %s, 'slickdeals.net')"""

			with con:
				# logging.debug(dealText)
				for m in dealText:
					try:
						eachOne = BeautifulSoup(str(m), "html5lib")
						dealid = eachOne.find("div", {"class":"coupon-content  "})['data-threadid']
						#logging.debug('DEAL ID: [%s]' % dealid)
						# url = eachOne.find("a", href=re.compile('^/f/'+str(dealid)+'-'))['href']
						url = '/f/'+str(dealid)
						#logging.debug('URL: [%s]' % url)
						# content = eachOne.find("div", {"class":"truncateTitle threeLine "}).find("a", href=re.compile('^/f/'+str(dealid)+'-')).contents[0]
						content = eachOne.find("div", {"class":"dealTitle truncateTitle "}).find("a", href=re.compile('^/f/'+str(dealid)+'-')).contents[0]
						#logging.debug('content [%s]' % content)
						#content2 = eachOne.find("div", {"class":"priceShippingTruncate "}).find("span", {"class":"price"}).find("a", href=re.compile('^/f/'+str(dealid)+'-')).contents[0]
						content2 = eachOne.find("a", {"class":"priceShippingTruncate "}).find("span", {"class":"price"}).contents[0]
						content += ' '+content2
						#logging.debug('content full [%s]' % content)
						if url is None:
							break
							
						try:
							cur.execute(insertQuery, (self.urlstr+url, content, ''))
							# if self.es != None:
							# 	try:
							# 		doc = {
							# 			'site':'slickdeals.net',
							# 			'url':'http://www.slickdeals.net'+url,
							# 			'content':content,
							# 			'reg_dtime':str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
							# 		}
							# 		res = self.es.index(index="deals", doc_type='deal', body=doc)
							# 		logging.debug(res)
							# 	except Exception as e:
							# 		logging.exception(e)
						except exc.SQLAlchemyError as me:
							logging.debug('duplicated record')
						except Exception as e:
							logging.exception(e)
							#logging.debug('duplicated')
					except (NameError, TypeError, AttributeError):
						logging.debug('not found')
					except Exception as e:
						logging.exception(e)

			con.close()

		except Exception as e:
			logging.exception(e)

