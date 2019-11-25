# coding=utf-8
# /usr/bin/env pythpn

'''
Author: yinhao
Email: yinhao_x@163.com
Wechat: xss_yinhao
Github: http://github.com/yinhaoxs

data: 2019-11-25 11:27
desc:
'''

import base64, os, io, time, sys
import cv2
from PIL import Image
import numpy as np
import argparse

import happybase
from thrift.transport import TSocket, TTransport
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol, TCompactProtocol


class HBase_Operation():
	def __init__(self, host="30.105.10.146"):
		self.connection = happybase.Connection(host=host, port=9090, autoconnect=True, timeout=None, transport='buffered')

	def create_table(self, table):
		self.connection.create_table(table, {"data": {}})

	def delete_table(self, table):
		self.connection.delete_table(table, True)

	def check_table(self, table):
		t = self.connection.table(table)
		regions = t.regions()
		print("regions of table: {}".format(regions))
		print("num regions: {}".format(len(regions)))
		num_entry = 0
		for region in regions:
			start_key = region["start_key"]
			end_key = region["end_key"]
			for k, v in t.scan(row_start=start_key, row_stop=end_key):
				print("rowkey: {}".format(k))
				num_entry+=1
			print("number of entry: {}".format(num_entry))

	def refactor_table(self, table):
		self.connection.delete_table(table)
		self.connection.create_table(table)

	def query_all_table_name(self):
		table_list = self.connection.tables()
		return table_list

	def shutdown(self):
		self.connection.close()


if __name__ == "__main__":
	hbase_op = HBase_Operation()

	operation = sys.argv[1]
	table = ''
	if not operation:
		print("please specify the operation.")
		hbase_op.shutdown()
		sys.exit(-1)

	elif operation != "query":
		table = sys.argv[2]

	if operation == "create":
		hbase_op.create_table(table=table)
		print("create table {}".format(table))

	elif operation == "delete":
		hbase_op.delete_table(table=table)

	elif operation == "check":
		hbase_op.check_table(table=table)

	elif operation == "query":
		table_list = hbase_op.query_all_table_name()
		for table in table_list:
			print(table+"\n")

	elif operation == "refactor":
		hbase_op.refactor_table(table)

	else:
		print("please specify the correct operations.")

	hbase_op.shutdown()


