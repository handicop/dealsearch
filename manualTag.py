#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy import exc
import sys
from sets import Set
import telepot
import logging
import json
reload(sys)
sys.setdefaultencoding('utf-8')

class ManualTag:
	selectTagZero = """ select id, content from deal_history where (tag1 is null or (tag2 is null and site in ('ppomppu.co.kr', 'clien.net'))) and date(reg_dtime) = curdate() union all select id, content from deal_history where tag1 is null and date(reg_dtime) = curdate() """
	bot = None
	phil = None
	lastTagRequestIdNum = 0
	def __init__(self):
		config = {}
		
		try:
			with open('/home/pi/Documents/DealProject/config.json', 'r') as f:
				config = json.load(f)
		except Exception as e:
			logging.exception(e)
		
		logging.info('manual tag class initialized')
		self.engine = create_engine(config['mysql'])
		self.bot = telepot.Bot(config['manual_tag_bot'])
		self.phil = config['phil_telegram_id']

	def sendMessage(self, dealId, content):
		try:
			if dealId > self.lastTagRequestIdNum:
				self.bot.sendMessage(self.phil, '{}@@@{}'.format(dealId, content))
				self.lastTagRequestIdNum = dealId
			return True
		except Exception as e:
			logging.exception(e)
			return False

	def run(self):
		con = None
		try:
			con = self.engine.connect()
			result = con.execute(self.selectTagZero)
			logging.info('# of no tagged records count[{}]'.format(result.rowcount))
			for r in result:
				self.sendMessage(r[0], r[1])
				logging.debug('message sent for manual tagging [{}]'.format(r[1]))

		except Exception as e:
			logging.exception(e)
		finally:
			if con != None:
				con.close()