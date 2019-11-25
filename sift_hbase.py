# coding=utf-8
# /usr/bin/env pythpn

'''
Author: yinhao
Email: yinhao_x@163.com
Wechat: xss_yinhao
Github: http://github.com/yinhaoxs

data: 2019-11-25 10:20
desc:
'''

import dill
import os, codecs, json,io, sys, time, random
from collections import OrderedDict
from torchvision import transforms
import cv2
from PIL import Image
import numpy as np

import happybase, base64
# from hbase import Hbase
# from hbase.ttypes import *
from thrift.transport import TSocket, TTransport
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol, TCompactProtocol

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark import SparkContext, SparkConf
# reload(sys)
# sys.setdefaultencoding("utf-8")


def write_sift_for_each_partition(keys):
	sift_count = 80
	connection = happybase.Connection(host="30.105.10.146",
									  port=9090,
									  autoconnect=False,
									  timeout=None,
									  transport='buffered',
									  protocol='binary')
	connection.open()
	image_table = connection.table('claim:table1')
	feature_table = connection.table('claim:table2')

	for key in keys:
		for img_id, bindata in image_table.scan(row_start=key[0], row_stop=key[1]):
			extractor = cv2.xfeatures2d.SIFT_create()
			sift = []

			try:
				iobuf = io.BytesIO(bindata["data:image"])
				im = Image.open(iobuf)
				im = np.asarray(im)
				im = cv2.cvtColor(np.asarray(im), cv2.COLOR_BGR2RGB)

				keypoints, features = extractor.detectAndCompute(im, None)
				if len(keypoints) == 0:
					print("error: img:{}".format(img_id))
					ff = [(kp.response, fe) for kp, fe in zip(keypoints, features)]
					sf = sorted(ff, key=lambda item: item[0], reverse=True)
					num_sift = sift_count if len(sf) > sift_count else len(sf)
					for res, fe in sf[:num_sift]:
						fe = [int(_) for _ in fe]
						sift.append(fe)

				# write feature
				with feature_table.batch(batch_size=128) as bat:
					bytesio = io.BytesIO()
					np.savetxt(bytesio, sift)
					content = bytesio.getvalue()
					b64_code = base64.b64encode(content)
					bat.put(row=img_id, data={"data:sift": b64_code})

			except Exception as e:
				print("error: img: {}".format(img_id, str(e)))

		connection.close()


if __name__ == "__mian__":
	# config
	sc = SparkSession.builder.appName("PySpark").getOrCreate()

	# build connection
	conection = happybase.Connection(host="30.105.10.146",
									  port=9090,
									  autoconnect=False,
									  timeout=None,
									  transport='buffered',
									  protocol='binary')

	conection.open()

	# open table
	table = conection.table('claim:table1')

	# get regions
	regions = table.regions()
	num_region = len(regions)

	# get start key
	keys = []
	for region in regions:
		keys.append((region["start_key"], region["end_key"]))

	# close connection
	conection.close()

	# start spark session
	rdd = sc.sparkContext.parallelize(keys, num_region)
	rdd.foreachPatition(write_sift_for_each_partition)









