#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy import exc
import urllib2
from bs4 import BeautifulSoup
# from elasticsearch import Elasticsearch
import re
import datetime
import logging
import sys
import json
reload(sys)
sys.setdefaultencoding('utf-8')

class Clien:

	engine = None
	urlstr = None
	insertQuery = """insert into deal_history (`url`, `content`, `img`, `site`) values (%s, %s, %s, 'clien.net')"""
	es = None

	def __init__(self):
		logging.info('Clien initialized')
		config = {}
		
		try:
			with open('/home/pi/Documents/DealProject/config.json', 'r') as f:
				config = json.load(f)
		except Exception as e:
			logging.exception(e)
		
		self.headers = {'User-Agent':'Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_2_1 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8G4 Safari/6533.18.5'}
		self.engine = create_engine(config['mysql'])
		self.urlstr = config['clien_url']

		
		# try:
		# 	self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
		# except Exception, e:
		# 	logging.error(e)
		# 	self.es = None

	def run(self):
		con = None
		try:
			# print('inside of run')
			con = self.engine.connect()
			for index in range(1, 3):
				self.process(self.urlstr+''+str(index), con)
			# print('end of range for loop')
		except Exception as e:
			logging.exception(e)
			# print(e)
			# print(e)
		finally:
			if con != None:
				con.close()

	def contentFilter(self, content):
		try:
			if '&page=' in content:
				return content[0:content.index('&page=')]
			else:
				return content
		except Exception as e:
			return content

	def process(self, url2Process, con):
		req = urllib2.Request(url2Process, None, self.headers)
		htmlSource = urllib2.urlopen(req).read()

		soup = BeautifulSoup(htmlSource, "html5lib")
		dealText = soup.findAll("div", {"class":"wrap_tit scalable"})

		# deal count is less than 1 then quit
		if len(dealText) < 1:
			logging.warn('deal qty is less than 1')
			return
		# print(len(dealText))
		# print(dealText[0]['href'])

		logging.info('deal qty %d' % len(dealText))
		try:
			for m in dealText:
				try:
					eachOne = BeautifulSoup(str(m), "html5lib")
					content = eachOne.find("div", {"class":"wrap_tit scalable"})['onclick']
					content2 = eachOne.find("span", {"class":"lst_tit"}).contents[0]
					if content2.startswith('[공지]') or content2.startswith('[쇼핑몰]'):
						continue
					# http://m.clien.net/cs3/board?bo_table=jirum&bo_style=view&wr_id=514160
					# location.href='?bo_table=jirum&bo_style=view&wr_id=259435&page=1'
					tableId = content[content.index('&wr_id=')+7:content.index('&page=')]
					# print 'id: {} content: {}'.format(content[content.index('&wr_id=')+7:content.index('&page=')], content2)
					url = 'http://m.clien.net/cs3/board?bo_table=jirum&bo_style=view&wr_id='+ tableId
					try:
						con.execute(self.insertQuery, (url, content2, '' ))
						# if self.es != None:
						# 	try:
						# 		doc = {
						# 			'site':'clien.net',
						# 			'url':url,
						# 			'content':content2,
						# 			'reg_dtime':str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
						# 		}
						# 		res = self.es.index(index="deals", doc_type='deal', body=doc)
						# 		logging.debug(res)
						# 	except Exception as e:
						# 		logging.exception(e)
					except exc.SQLAlchemyError as se:
						logging.debug('duplicated keyword')
						# print(se)
					except Exception as e:
						logging.exception(e)
						# print(e)
				except (NameError, TypeError, AttributeError):
					logging.debug('not found')
				except Exception as e:
					logging.exception(e)					
					# print(e)

		except Exception as e:
			logging.exception(e)
			# print(e)


