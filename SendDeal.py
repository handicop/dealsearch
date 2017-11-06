#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy import exc
from Daemon import Daemon
import sys, time
import pickle
from datetime import datetime
from sets import Set
import telepot
import logging
import json
from MyLog import MyLog
reload(sys)
sys.setdefaultencoding('utf-8')

class SendDeal(Daemon):
	engine = None
	bot = None
	con = None
	# selectUserKeyword = """ select user_id, keyword from deal_user_keyword where last_inquiry_dtime < ( select max(reg_dtime) from deal_history where date(reg_dtime) = curdate() limit 1) order by user_id """
	# selectFeedInquiry = """ SELECT url, content, reg_dtime FROM deal_history WHERE date(reg_dtime) = curdate() and %s in (tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8) order by reg_dtime desc"""
	selectUserKeyword = """ select user_id, keyword, last_inquiry_dtime from deal_user_keyword order by user_id """
	selectFeedInquiry = """ SELECT url, content, reg_dtime FROM deal_history 
							WHERE reg_dtime > %s and 
							(
							%s in (tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8) or
							(select tag2 from keyword_dict where tag1 = %s) in (tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8) or
							(select tag3 from keyword_dict where tag1 = %s) in (tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8) or
							(select tag4 from keyword_dict where tag1 = %s) in (tag1, tag2, tag3, tag4, tag5, tag6, tag7, tag8) 
							)
							and date(reg_dtime) = curdate() order by reg_dtime desc """
	updateQuery = """ UPDATE deal_user_keyword SET last_inquiry_dtime = current_timestamp() where user_id = %s and keyword = %s """

	def run(self):
		mylog = MyLog('/home/pi/Documents/DealProject/log/senddeal.log')
		config = {}
		
		try:
			with open('/home/pi/Documents/DealProject/config.json', 'r') as f:
				config = json.load(f)
		except Exception as e:
			logging.exception(e)

		self.bot = telepot.Bot(config['manual_tag_bot'])
		self.engine = create_engine(config['mysql'])

		while True:
			self.runSendMessage()
			logging.info('sleep')
			time.sleep(60)

	def runSendMessage(self):
		# key: userid
		# value: set of deal url
		dic = {}
		try:
			logging.info('running send message process')
			self.con = self.engine.connect()
			result = self.con.execute(self.selectUserKeyword)
			if result.rowcount > 0:
				logging.info('result row count {}'.format(result.rowcount))
				for row in result:
					result2 = self.con.execute(self.selectFeedInquiry, row[2] if row[2] != None else '0000-00-00 00:00:00', row[1], row[1], row[1], row[1])
					logging.info('result row count [{}] for matching keyword [{}] user [{}] last_inquiry_dtime [{}]'.format(result2.rowcount, row[1], row[0], row[2]))
					if result2.rowcount > 0:
						# matching deals found
						for row2 in result2:
							# add new set for user id row[0]
							if row[0] not in dic:
								dic[row[0]] = Set()
							dic[row[0]].add(row2[0])
						self.updateHistory(row[0], row[1], row2[2])
			logging.info('# of user to send msg: {}'.format(len(dic)))
			# if there's any key(user_id), then send msg
			for d in dic:
				self.sendBulkMsg(d, dic[d])

		except Exception as e:
			logging.exception(e)
		finally:
			if self.con != None:
				self.con.close()
				self.con = None
			dic = None

	def sendBulkMsg(self, userId, linkset):
		for link in linkset:
			self.sendMsg(userId, link)

	def sendMsg(self, userId, link):
		try:
			self.bot.sendMessage(userId, link)
		except Exception as e:
			logging.exception(e)

	def updateHistory(self, userId, keyword, lastTime):
		try:
			result = self.con.execute(self.updateQuery, (userId, keyword))
			if result.rowcount > 0:
				logging.info('good - update send msg history for user id [{}] with keyword [{}]'.format(userId, keyword))
			else:
				logging.warn('failed - update send msg history for user id [{}] with keyword [{}]'.format(userId, keyword))
		except Exception, e:
			raise e

if __name__ == "__main__":
	daemon = SendDeal('/home/pi/Documents/DealProject/senddeal.pid')
	if len(sys.argv) == 2:
		if 'start' == sys.argv[1]:
			daemon.start()
		elif 'stop' == sys.argv[1]:
			daemon.stop()
		elif 'restart' == sys.argv[1]:
			daemon.restart()
		else:
			print "Unknown command"
			sys.exit(2)
			sys.exit(0)
	else:
		print "usage: %s start|stop|restart" % sys.argv[0]
		sys.exit(2)
