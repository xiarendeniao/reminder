#encoding=utf-8
import json, time, sys
from datetime import datetime
from pprint import pprint
from twisted.internet import reactor, protocol, task 
from twisted.enterprise import adbapi
from twisted.python import log
from txredis.client import RedisClient, RedisSubscriber
from config import *

#global variables
g_redis = None
g_redisSub = None
g_mysql = None
g_todos = dict()

def SendMsg(record):
    log.msg('to send "%s"' % str(record))
    #... fix me. markbyxds
    return True

def NextScheduleTime(record):
    #import pdb; pdb.set_trace()
    now = time.time()
    if record['time'] >= now:
        return record['time'] - now
    else:
        oriDt = datetime.fromtimestamp(record['time'])
        nowDt = datetime.fromtimestamp(now)
        assert(record['cycle'] != TODO_CYCLE_NONE)
        if record['cycle'] == TODO_CYCLE_DAY:
            nextDt = datetime(nowDt.year, nowDt.month, nowDt.day, oriDt.hour, oriDt.minute, oriDt.second)
            if nextDt >= nowDt:
                delta = nextDt - nowDt
                return delta.days*24*3600 + delta.seconds
            else:
                return int(nextDt.strftime('%s')) + 24*3600 - now
        elif record['cycle'] == TODO_CYCLE_WEEK:
            mondayT = now - nowDt.weekday()*24*3600 - nowDt.hour*3600 - nowDt.minute*60 - nowDt.second #monday 00:00:00
            nextT = mondayT + oriDt.weekday()*24*3600 + oriDt.hour*3600 + oriDt.minute*60 + oriDt.second
            if nextT >= now:
                return nextT - now
            else:
                return nextT + 7*24*3600 - now
        elif record['cycle'] == TODO_CYCLE_MONTH:
            def samedaydt(monthDt):
                day = oriDt.day
                while True:
                    try: nextDt = datetime(monthDt.year, monthDt.month, day, oriDt.hour, oriDt.minute, oriDt.second)
                    except Exception, e: log.msg('samedaydt: %r' % e); day -= 1
                    else: return nextDt
            nextDt = samedaydt(nowDt)
            if nextDt < nowDt:
                if nowDt.month == 12: nextMonthDt = datetime(nowDt.year+1, 1, 1)
                else: nextMonthDt = datetime(nowDt.year, nowDt.month+1, 1)
                nextDt = samedaydt(nextMonthDt)
            delta = nextDt - nowDt
            return delta.days*24*3600 + delta.seconds
        elif record['cycle'] == TODO_CYCLE_YEAR:
            def samedaydt2(yearDt):
                day = oriDt.day
                while True:
                    try: nextDt = datetime(yearDt.year, oriDt.month, day, oriDt.hour, oriDt.minute, oriDt.second)
                    except Exception,e: log.msg('samedaydt2: %r' % e); day -= 1
                    else: return nextDt
            nextDt = samedaydt2(nowDt)
            if nextDt < nowDt:
                nextYearDt = datetime(nowDt.year+1, 1, 1)
                nextDt = samedaydt2(nextYearDt)
            delta = nextDt - nowDt
            return delta.days*24*3600 + delta.seconds
        else:
            log.msg('invalid cycle: %s' % record)
            return None

def GenerateTask(record):
    log.msg('to generate task for %s' % record)
    def cb(sendRt):
        log.msg('rt of record %s: %s' % (str(record), sendRt))
        if record['cycle'] != TODO_CYCLE_NONE:
            deferTask = GenerateTask(record)
            if deferTask:
                g_todos[record['todoId']] = deferTask
        else:
            del g_todos[record['todoId']]
    def err(e):
        log.msg('send %s failed: %r' % (str(record), e))
        #what about db? markbyxds 
        del g_todos[record['todoId']]
    seconds = NextScheduleTime(record)
    if seconds == None:
        return None
    log.msg('executed task %ss later: %s' % (seconds, record))
    sendTask = task.deferLater(reactor, seconds, SendMsg, record)
    sendTask.addCallbacks(cb, err)
    return sendTask

def TodoMod(record):
    #pprint(g_todos)
    if record['todoId'] in g_todos: 
        g_todos[record['todoId']].cancel()
    deferTask = GenerateTask(record)
    if deferTask:
        g_todos[record['todoId']] = deferTask 
        log.msg('modified record %s' % record['todoId'])

def TodoAdd(record):
    deferTask = GenerateTask(record)
    if deferTask:
        g_todos[record['todoId']] = deferTask
        log.msg('added a record %s' % record['todoId'])

def TodoDel(record):
    if record['todoId'] not in g_todos: #db data not equal sent data. fix me. markbyxds  
        log.msg('to mod a nonexists todo %s, ignored' % record['todoId'])
        return
    g_todo[record['todoId']].cancel()
    
class MySubScriber(RedisSubscriber):
    def messageReceived(self, channel, message):
        log.msg('subscriber %s responsed: %s' % (channel, message))
        assert(channel == NEW_PUBLISH)
        def err(e):
            log.msg('lpop(%s) failed: %r' % (TODO_Q, e))
            #raise e
        def cb(todo):
            if todo != None:
                try:
                    todo = json.loads(todo)
                except Exception,e:
                    log.msg('invalid json encoded data: %s' % todo)
                if todo[0] == 'mod': TodoMod(todo[1])
                elif todo[0] == 'del': TodoDel(todo[1])
                elif todo[0] == 'add': TodoAdd(todo[1])
                else: log.msg('unrecognized todo task %s' % str(todo))
                g_redis.rpop(TODO_Q).addCallbacks(cb, err)
        if message == 'todo':
            g_redis.rpop(TODO_Q).addCallbacks(cb, err) 
    
def ConnectToRedis():
    def err(e):
        log.msg('connect redis failed: %r' % e)
        reactor.stop()
    def cb1(redisObj):
        global g_redis; g_redis = redisObj
        log.msg('established redis connection')
    def cb2(redisSub):
        global g_redisSub; g_redisSub = redisSub
        log.msg('subscribed redis ')
        g_redisSub.subscribe(NEW_PUBLISH)
    #redis client
    clientCreator = protocol.ClientCreator(reactor, RedisClient)
    clientCreator.connectTCP(REDIS_HOST, REDIS_PORT).addCallbacks(cb1, err)
    #redis subscriber
    clientCreator = protocol.ClientCreator(reactor, MySubScriber)
    clientCreator.connectTCP(REDIS_HOST, REDIS_PORT).addCallbacks(cb2, err)
    
def Initialize():
    def err(e):
        reactor.stop()
    def cb(sqlRts):
        #parse db data
        for sqlRt in sqlRts:
            record = dict()
            record['todoId'] = sqlRt[0]
            record['userId'] = sqlRt[1]
            record['type'] = sqlRt[2]
            record['todo'] = sqlRt[3]
            record['time'] = sqlRt[4]
            record['cycle'] = sqlRt[5]
            #import pdb; pdb.set_trace()
            deferTask = GenerateTask(record)
            if deferTask:
                g_todos[record['todoId']] = deferTask
        log.msg('initializ data from db finished')
        #connect redis
        ConnectToRedis()
    g_mysql.runQuery('SELECT id,userId,type,todo,time,cycle FROM t_todo WHERE time > UNIX_TIMESTAMP(now()) OR cycle != %s', 
                     (TODO_CYCLE_NONE,)).addCallbacks(cb,err)

if __name__ == '__main__':
    #log
    log.startLogging(sys.stderr)
    
    #mysql
    def cb(result):
        log.msg('established a connection to mysql')
    g_mysql = adbapi.ConnectionPool("MySQLdb", host = MYSQL_HOST, port = MYSQL_PORT, user = MYSQL_USER, passwd = MYSQL_PASSWD, 
                            db = MYSQL_DB, charset = 'utf8', cp_min=5, cp_max=10, cp_reconnect=True, cp_openfun=cb)
    
    #redis
    callId = reactor.callLater(0, Initialize)
    
    #start reactor
    reactor.run()