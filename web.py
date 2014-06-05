#encoding=utf-8
import json, time, sys
from twisted.internet import task 
from twisted.web import server
from twisted.internet import reactor
from twisted.web.resource import Resource
from twisted.internet import protocol
from twisted.enterprise import adbapi
from twisted.python import log
#https://github.com/deldotdr/txRedis
from txredis.client import RedisClient
from config import *

#global variables
g_redis = None
g_mysql = None

def RequestArgError(argName, errString):
	log.msg('argement %s : %s' % (argName, errString))
	return json.dumps({'rt':1, 'data':{'argv':argName, 'error':errString}})

def ServerInnerError(errString):
	return json.dumps({'rt':500, 'data':errString})

def RequestOk(string = ""):
	return json.dumps({'rt':0, 'data':string})

class UserNew(Resource):
	'''
	new user
	'''
	def render(self, request):
		#parse argument
		if 'id' not in request.args:
			return RequestArgError('id', 'not found')
		userId = request.args['id'][0]
		if not userId.isdigit():
			return RequestArgError('id', 'invalid')
		userId = int(userId)
		
		def err(e):
			log.msg('uri "%s" failed: %r' % (request.uri, e))
			request.write(ServerInnerError('%r'%e))
			request.finish()
			#raise e
			
		def cb2(result):
			#import pdb; pdb.set_trace()
			request.write(RequestOk({'status':0}))
			request.finish()
				
		def cb1(result):
			if not result:
				g_mysql.runQuery('INSERT INTO t_user(id) VALUES (%s)', (userId,)).addCallbacks(cb2, err)
			else:
				request.write(RequestOk({'status':1}))
				request.finish()

		g_mysql.runQuery('SELECT id FROM t_user WHERE id = %s', (userId,)).addCallbacks(cb1, err) #optimize: query from redis! markbyxds 
		return server.NOT_DONE_YET
		
class TodoAdd(Resource):
	'''
	add a todo
	'''
	def render(self, request):
		#parse parameter 
		record = dict()
		args = request.args
		try:
			record['userId'] = int(args['userId'][0]) #what about nonexists userId? markbyxds
			record['type'] = int(args['type'][0].strip()) #default 2
			record['todo'] = args['todo'][0].strip()
			record['time'] = int(args['time'][0])
			record['cycle'] = int(args['cycle'][0]) #default TODO_CYCLE_NONE
		except Exception,e:
			log.msg('parameters invalid. %r' % e)
			return RequestArgError('', 'parameters invalid. %r' % e)
		#call backs
		def err(e):
			log.msg('uri "%s" failed: %r' % (request.uri, e))
			request.write(ServerInnerError('%r'%e))
			request.finish()
			#raise e #required or not!? markbyxds 
		def cb3(receiverNum):
			log.msg('redis %s receiver num： %s' % (NEW_PUBLISH, receiverNum))
			request.write(RequestOk())
			request.finish()
		def cb2(queueLength, record):
			log.msg('redis %s length： %s' % (TODO_Q, queueLength))
			g_redis.publish(NEW_PUBLISH, 'todo').addCallbacks(cb3, err)
		def cb1(record):
			if record == None:
				request.write(ServerInnerError('insert todo to db failed'))
				request.finish()
			else:
				log.msg('last insert id for t_todo : %s' % record['id'])
				g_redis.lpush(TODO_Q, json.dumps(('add',record))).addCallbacks(cb2, err, (record,), {}, (), {})
		def addtodo(txn, record):
			txn.execute('INSERT INTO t_todo (userId,type,todo,time,cycle) VALUES (%s,%s,%s,%s,%s)', 
					(record['userId'], record['type'], record['todo'], record['time'], record['cycle']))
			txn.execute('SELECT LAST_INSERT_ID()')
			result = txn.fetchall()
			if result:
				record['id'] = result[0][0]
				return record
			else: return None
		#insert into db
		g_mysql.runInteraction(addtodo, record).addCallbacks(cb1, err)
		return server.NOT_DONE_YET

class TodoDel(Resource):
	'''
	del a todo
	'''
	def render(self, request):
		#parse parameters
		args = request.args
		record = dict()
		try:
			record['todoId'] = int(args['todoId'][0])
		except Exception,e:
			log.msg('%r' % e)
			return RequestArgError('', '%r' % e)
		#call backs
		def err(e):
			log.msg('uri "%s" failed: %r' % (request.uri, e))
			request.write(ServerInnerError('%r'%e))
			request.finish()
			#raise e
		def cb3(receiverNum):
			log.msg('redis %s receiver num： %s' % (NEW_PUBLISH, receiverNum))
			request.write(RequestOk())
			request.finish()
		def cb2(queueLength):
			log.msg('redis %s length： %s' % (TODO_Q, queueLength))
			g_redis.publish(NEW_PUBLISH, 'todo').addCallbacks(cb3, err)
		def cb1(sqlRt, record):
			g_redis.lpush(TODO_Q, json.dumps(('del',record))).addCallbacks(cb2, err)
		#del todo
		g_mysql.runOperation('DELETE FROM t_todo WHERE id = %s', (record['todoId'],)).addCallbacks(cb1, err, (record,))
		return server.NOT_DONE_YET

class TodoMod(Resource):
	'''
	modify a todo
	'''
	def render(self, request):
		#parse parameters
		args = request.args
		record = dict()
		try:
			record['todoId'] = int(args['todoId'][0])
			record['userId'] = int(args['userId'][0])
			record['type'] = int(args['type'][0].strip())
			record['todo'] = args['todo'][0].strip()
			record['time'] = int(args['time'][0])
			record['cycle'] = int(args['cycle'][0])
		except Exception,e:
			log.msg('%r' % e)
			return RequestArgError('', '%r' % e)
		#call backs
		def err(e):
			log.msg('uri "%s" failed: %r' % (request.uri, e))
			request.write(ServerInnerError('%r'%e))
			request.finish()
			#raise e
		def cb4(receiverNum):
			log.msg('redis %s receiver num： %s' % (NEW_PUBLISH, receiverNum))
			request.write(RequestOk())
			request.finish()
		def cb3(queueLength, record):
			log.msg('redis %s length： %s' % (TODO_Q, queueLength))
			g_redis.publish(NEW_PUBLISH, 'todo').addCallbacks(cb4, err)
		def cb2(sqlRt, record):
			g_redis.lpush(TODO_Q, json.dumps(('mod',record))).addCallbacks(cb3, err, (record,), {}, (), {})
		def cb1(sqlRt, record):
			if not sqlRt:
				request.write(RequestArgError('todoId', 'not found in db'))
				request.finish()
			else:
				#update db
				g_mysql.runOperation('UPDATE t_todo SET userId=%s, type=%s, todo=%s, time=%s, cycle=%s WHERE id=%s', 
							(record['userId'], record['type'], record['todo'], record['time'], record['cycle'], 
							record['todoId'])).addCallbacks(cb2, err, (record,))
		g_mysql.runQuery('SELECT id FROM t_todo WHERE id = %s', (record['todoId'],)).addCallbacks(cb1, err, (record,))
		return server.NOT_DONE_YET
	
class TodoIds(Resource):
	'''
	query todo ids
	'''
	def render(self, request):
		#parse parameters
		args = request.args
		try:
			userId = int(args['userId'][0])
			expired = int(args['expired'][0])
			assert(expired in (0,1))
		except Exception,e:
			log.msg('%r' % e)
			return RequestArgError('', '%r' % e)
		#call backs
		def err(e):
			log.msg('uri "%s" failed: %r' % (request.uri, e))
			request.write(ServerInnerError('%r'%e))
			request.finish()
			#raise e
		def cb1(sqlRt):
			rt = {'rt':0, 'data':[]}
			if sqlRt:
				rt['data'] = [tmpRt[0] for tmpRt in sqlRt]
			request.write(json.dumps(rt))
			request.finish()
		#query from db
		if expired == 1:
			#expired
			g_mysql.runQuery('SELECT id FROM t_todo WHERE userId = %s AND time < unix_timestamp(now())', 
							(userId,)).addCallbacks(cb1, err)
		else:
			#not expired
			g_mysql.runQuery('SELECT id FROM t_todo WHERE userId = %s AND time >= unix_timestamp(now())', 
							(userId,)).addCallbacks(cb1, err)
		return server.NOT_DONE_YET
	
class TodoDetail(Resource):
	'''
	query todo detail
	'''
	def render(self, request):
		#parse parameters
		args = request.args
		try:
			todoId = int(args['todoId'][0])
		except Exception,e:
			log.msg('%r' % e)
			return RequestArgError('', '%r' % e)
		#call backs
		def err(e):
			log.msg('uri "%s" failed: %r' % (request.uri, e))
			request.write(ServerInnerError('%r'%e))
			request.finish()
			#raise e
		def cb1(sqlRt):
			if not sqlRt:
				request.write(RequestArgError('todoId', 'not found in db'))
				request.finish()
			else:
				rt = {'rt':0, 'data':{}}
				sqlRt = sqlRt[0]
				rt['data']['id'] = sqlRt[0]
				rt['data']['userId'] = sqlRt[1]
				rt['data']['type'] = sqlRt[2]
				rt['data']['todo'] = sqlRt[3]
				rt['data']['time'] = sqlRt[4]
				rt['data']['cycle'] = sqlRt[5]
				#import pdb; pdb.set_trace()
				request.write(json.dumps(rt))
				request.finish()
		#query from db
		g_mysql.runQuery('SELECT id,userId,type,todo,time,cycle FROM t_todo WHERE id = %s', (todoId,)).addCallbacks(cb1, err)
		return server.NOT_DONE_YET
	
def InitMainResource():	
	#root resource
	rootRes = Resource()
	
	#/user /reminder
	userRes = Resource()
	rootRes.putChild('user', userRes)
	todoRes = Resource()
	rootRes.putChild('todo', todoRes)
	
	#/user/new
	userRes.putChild('new', UserNew())
	
	#/todo/add
	todoRes.putChild('add', TodoAdd())
	#/todo/del
	todoRes.putChild('del', TodoDel())
	#/todo/mod
	todoRes.putChild('mod', TodoMod())
	#/todo/ids
	todoRes.putChild('ids', TodoIds())
	#/todo/detail
	todoRes.putChild('detail', TodoDetail())
	
	return rootRes

def ConnectToRedis():
	def cb1(redisObj):
		global g_redis
		g_redis = redisObj
		#import pdb; pdb.set_trace()
		log.msg('established redis connection')
		#web
		site = server.Site(InitMainResource())
		reactor.listenTCP(8000, site)
	def err1(e):
		log.msg('connect redis failed: %r' % e)
		reactor.stop()
	clientCreator = protocol.ClientCreator(reactor, RedisClient)
	clientCreator.connectTCP(REDIS_HOST, REDIS_PORT).addCallbacks(cb1, err1)

if __name__ == '__main__':	
	#log
	log.startLogging(sys.stdout)
	
	#redis
	callId = reactor.callLater(0, ConnectToRedis)
	
	#mysql
	def cb(result):
		log.msg('established a connection to mysql')
	g_mysql = adbapi.ConnectionPool("MySQLdb", host = MYSQL_HOST, port = MYSQL_PORT, user = MYSQL_USER, passwd = MYSQL_PASSWD, 
							db = MYSQL_DB, charset = 'utf8', cp_min=5, cp_max=10, cp_reconnect=True, cp_openfun=cb)
	
	#start reactor
	reactor.run()