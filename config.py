#encoding=utf-8

#redis server config
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

#mysql server config
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWD = '123456'
MYSQL_DB = 'reminder'

#global const variables
TODO_Q = 'todo'
NEW_PUBLISH = 'new-data'

#global const variables
TODO_CYCLE_NONE     = 0
TODO_CYCLE_DAY      = 1
TODO_CYCLE_WEEK     = 2
TODO_CYCLE_MONTH    = 3
TODO_CYCLE_YEAR     = 4
TODO_CYCLE_TYPES = (
    TODO_CYCLE_NONE,
    TODO_CYCLE_DAY,
    TODO_CYCLE_WEEK,
    TODO_CYCLE_MONTH,
    TODO_CYCLE_YEAR)