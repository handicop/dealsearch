#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, time
from Daemon import Daemon
from datetime import date
from dealsea import Dealsea
from slickdeals import Slickdeals
from MyLog import MyLog
from tagging import Tagging
from ppom import Ppomppu
from manualTag import ManualTag
from clien import Clien
import logging
reload(sys)
sys.setdefaultencoding('utf-8')

class MyDaemon(Daemon):

	def run(self):
		mylog = MyLog('/home/pi/Documents/DealProject/log/dealdaemon.log')
		dealsea = Dealsea()
		slickdeals = Slickdeals()
		tagging = Tagging()
		ppom = Ppomppu()
		sendManualTag = ManualTag()
		clien = Clien()
		while True:
			dealsea.run()
			logging.info('***************************DEALSEA DONE******************************')
			slickdeals.run()
			logging.info('***************************SLICKDEALS DONE******************************')
			ppom.run()
			logging.info('***************************PPOMPPU DONE******************************')
			clien.run()
			logging.info('***************************CLIEN DONE******************************')
			try:
				logging.info('***************************tagging started***************************')
				for index in range(8):
					if tagging.run() <= 0:
						break
					else:
						logging.info('***************************tagging again***************************')
				logging.info('***************************tagging ended***************************')
				# sendManualTag.run()
			except Exception as e:
				logging.exception(e)
			
			time.sleep(60*5) # 20 minutes

if __name__ == "__main__":
	daemon = MyDaemon('/home/pi/Documents/DealProject/deal.pid')
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