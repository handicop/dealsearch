#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import logging.handlers
import logging
reload(sys)
sys.setdefaultencoding('utf-8')

class MyLog:
	
	def __init__(self, filename):
		formatter = logging.Formatter('[%(asctime)s] [%(levelname)-5s] [%(filename)-17s] [%(lineno)3d] %(message)s')
		logHandler = logging.handlers.TimedRotatingFileHandler(filename, when="midnight")
		logHandler.setFormatter(formatter)
		logger = logging.getLogger()
		logger.addHandler( logHandler )
		logger.setLevel(logging.DEBUG)
