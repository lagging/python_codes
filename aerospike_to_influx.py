#!/usr/bin/env python

#usage:    python influxtestfinaly.py -b <aerospike server> -i <aerospike port> -d <influx server> -e <influx port>
# make sure in influxdb database exist
# Modules
import argparse
import sys
import time
import socket
import httplib,requests

#from influxdb import InfluxDBClient

parser = argparse.ArgumentParser()
parser.add_argument("-b"
                                        , "--base-node"
                                        , dest="base_node"
                                        , default='127.0.0.1'
                                        , help="Base host for collecting stats (default: %(default)s)")
                                        
                                        
parser.add_argument("-i"
                                        , "--info-port"
                                        , dest="info_port"
                                        , default=3000
                                        , help="PORT for Aerospike server (default: %(default)s)")

parser.add_argument("-r"
                                        , "--xdr-port"
                                        , dest="xdr_port"
                                        , default=3004
                                        , help="PORT for XDR server(default: %(default)s)")

parser.add_argument("-d"
                                        , "--dest-node"
                                        , dest="dest_node"
                                        , default="127.0.0.1"
                                        , help="Influx server for collecting stats (default: %(default)s)")
                                        
parser.add_argument("-e"
                                        , "--exit-port"
                                        , dest="exit_port"
                                        , default=8086
                                        , help="port for influx server (default: %(default)s)")

                                        

args = parser.parse_args()

CITRUSLEAF_SERVER = args.base_node
CITRUSLEAF_PORT = int(args.info_port)
CITRUSLEAF_SERVER_ID = socket.gethostname()
INFLUXDB_SERVER= args.dest_node
INFLUXDB_PORT= int(args.exit_port)

print CITRUSLEAF_SERVER
print CITRUSLEAF_PORT
print CITRUSLEAF_SERVER_ID
print INFLUXDB_SERVER
print INFLUXDB_PORT


user=None
password=None



try:
        import citrusleaf
except:
        raise Exception, "unable to load Citrusleaf/Aerospike library"
        sys.exit(-1)



def fun():
	
	msg=[]
	#now=int(time.time())
	now=str(  int(round(time.time() ))*1000000000)
	r=citrusleaf.citrusleaf_info(CITRUSLEAF_SERVER, CITRUSLEAF_PORT, 'statistics')
	
	if (-1 != r):
        	lines = []
        	for string in r.split(';'):
                	if string == "":
                        	continue
			if string.count('=') > 1:
                                continue
			name, value = string.split('=')
                        value = value.replace('false', "0")
                        value = value.replace('true', "1")
                        lines.append("%s,host=%s %s %s" % (name,CITRUSLEAF_SERVER_ID,value, now))
		msg.extend(lines)

	
	if 1==1:
		r=-1
		r = citrusleaf.citrusleaf_info(CITRUSLEAF_SERVER, CITRUSLEAF_PORT, 'latency:')	
		if (-1 != r) and not (r.startswith('error')):
			lines = []
			latency_type = ""
			header = []
			for string in r.split(';'):
				if len(string) == 0:
					continue
				if len(latency_type) == 0:
					 latency_type, rest = string.split(':', 1)
					 header = rest.split(',')
				else:
					val = string.split(',')
					for i in range(1, len(header)):
						name = latency_type + "." + header[i]
						name = name.replace('>', 'over_')
						name = name.replace('ops/sec', 'ops_per_sec')
						value = val[i]
						lines.append("%s,host=%s %s %s" % (name,CITRUSLEAF_SERVER_ID,value, now))
					latency_type = ""
					header=[]
			msg.extend(lines)
			
	if 1==1:
		r=-1
		r = citrusleaf.citrusleaf_info(CITRUSLEAF_SERVER, CITRUSLEAF_PORT, 'namespaces')
		if (-1 != r):
			namespaces = filter(None, r.split(';'))
			if len(namespaces) > 0:
				for namespace in namespaces:
					r=-1
					try:
						r = citrusleaf.citrusleaf_info(CITRUSLEAF_SERVER, CITRUSLEAF_PORT, 'namespace/' + namespace)
					except:
						pass
					if (-1 != r):
						lines = []
						for string in r.split(';'):
							name, value = string.split('=')
							value = value.replace('false', "0")
							value = value.replace('true', "1")
							lines.append("%s,host=%s %s %s" % (name,CITRUSLEAF_SERVER_ID,value, now))
					msg.extend(lines)
	
	
	
	if 1==1:
		r=-1
		try:
			r = citrusleaf.citrusleaf_info(CITRUSLEAF_SERVER, CITRUSLEAF_XDR_PORT, 'statistics')
		except:
			pass
		if(-1 != r):
			lines = []
			for string in r.split(';'):
				if string == "":
					continue
					
				if string.count('=') > 1:
					continue
					
				name, value = string.split('=')
				value = value.replace('false', "0")
				value = value.replace('true', "1")
				lines.append("%s,host=%s %s %s" % (name,CITRUSLEAF_SERVER_ID,value, now))
			msg.extend(lines)
			
			
	
	if 1==1:
		r=-1
		try:
			r = citrusleaf.citrusleaf_info(CITRUSLEAF_SERVER, CITRUSLEAF_PORT, 'sindex')
		except:
			pass
		
		if(-1 != r):
			indexes = filter(None, r)
			if len(indexes) > 0:
				lines=[]
				for index_line in indexes.split(';'):
					if len(index_line) > 0:
						index = dict(item.split("=") for item in index_line.split(":"))
						
						if (index["sync_state"] == "synced"):
							index["sync_state"] = 1
						elif (index["sync_state"] == "need_sync"):
							index["sync_state"] = 0
							
						if (index["state"] == "RW"):
							index["state"] = 1
						elif (index["state"] == "WO"):
							index["state"] = 0
							
						lines.append("%s,host=%s %s %s" % (index["indexname"],CITRUSLEAF_SERVER_ID, index["sync_state"], now))
						lines.append("%s,host=%s %s %s" % (index["indexname"],CITRUSLEAF_SERVER_ID, index["state"], now))
						r=-1
						try:
							r = citrusleaf.citrusleaf_info(CITRUSLEAF_SERVER, CITRUSLEAF_PORT, 'sindex/' + index["ns"] + '/' + index["indexname"])
						except:
							pass
						if (-1 != r):
							for string in r.split(';'):
								name, value = string.split('=')
								value = value.replace('false', "0")
								value = value.replace('true', "1")
								lines.append("%s,host=%s %s %s" % (name,CITRUSLEAF_SERVER_ID,value, now))
				msg.extend(lines)
			
				
		
	
	
	


	nmsg=''
	for line in msg:
		fields=line.split()
		try:
			float(fields[1])
		except ValueError:
			val = fields[1]
			valstr = ''
			for x in val:
				try:
					int(x)
					valstr += str(x)
				except ValueError:
					valstr +=str(abs(ord(x.lower()) - 96))
			fields[1]=valstr
		#print type(fields[1])		
		#fields[1]="value="+`fields[1]`
		#print fields[1]
		fields[1]='value='+`float(fields[1])`
		line=''
		#msg  [cluster_size,host=54.255.192.165 value=113AB1 23424524,used-bytes-memory,host=54.255.192.165 value=0 1 93424524]
		#msg='used-bytes-memory,host=44.255.192.145 value=0 93424524000000\ncpu_load,host=server03 value=0.64 1434055562000000000\n'
		#msg='cpu_load_short,host=server02 value=0.64 1434055562000000000'
		for f in fields:
			line+=f+ ' '
		line=line[:-1]
		nmsg += line + '\n'
	print nmsg
	for abc in nmsg.split('\n'):
		#print abc
	#abc='cluster_size,host=ashish-Latitude-E5450 value=1 1442842201'
		tmp="http://localhost:8086/write?db=mydb"
		tmp=tmp.replace("localhost",INFLUXDB_SERVER)
		results2=requests.post(tmp,data=abc)
		print results2


	



	
if __name__ == "__main__":
	fun()

                                        
                               
