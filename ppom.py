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

class Ppomppu:

	engine = None
	urlstr = None
	urlstr_international = None
	insertQuery = """insert into deal_history (`url`, `content`, `img`, `site`, `tag1`) values (%s, %s, %s, 'ppomppu.co.kr', %s)"""
	es = None

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
		self.urlstr = config['ppom_url']
		self.urlstr_international = config['ppom_int_url']
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
			for index in range(1, 3):
				self.process(self.urlstr_international+''+str(index), con)
		except Exception as e:
			logging.exception(e)
			# print(e)
			# print(e)
		finally:
			if con != None:
				con.close()

	def tag1(self, content):
		try:
			if content.startswith('(끌올)'):
				content.replace('(끌올)', '', 1)
			if content.startswith('[끌올]'):
				content.replace('[끌올]', '', 1)
			if content.startswith('['):
				return content[1:content.index(']')]
			else:
				return None
		except Exception as e:
			logging.exception(e)
			return None

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
		dealText = soup.findAll("a", {"class":"list_b_01"})

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
					content = eachOne.find("span", {"class":"title"}).contents[0]
					if content.startswith('<span') or content.startswith('[쇼핑몰]'):
						continue
					url = 'http://m.ppomppu.co.kr/new/'+m['href']
					try:
						tempTag = self.tag1(str(content))
						con.execute(self.insertQuery, (self.contentFilter(url), content, '', tempTag ))
						# if self.es != None:
						# 	try:
						# 		doc = {
						# 			'site':'ppomppu.co.kr',
						# 			'url':self.contentFilter(url),
						# 			'content':content,
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


