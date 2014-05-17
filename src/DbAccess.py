#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import getopt
import warnings
import string
import MySQLdb as mysql
from MySQLdb import cursors
from RIP_DbSqlCompiler import *

# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbCommand
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbConnection(object):

    def __init__(self, config = None):
        if config is not None:
            self.__host = config.host
            self.__name = config.name
            self.__user = config.user
            self.__password = config.password
    
    # ------------------------------------------------------
    #   public properties 
    # ------------------------------------------------------

    # property: host [string]
    #   hostname of db-server
    __host = ''
    def _get_host(self):
        return self.__host
    def _set_host(self, value):
        self.__host = value
    host = property(_get_host, _set_host)

    # property: name [string]
    #   name of database
    __name = ''
    def _get_name(self):
        return self.__name
    def _set_name(self, value):
        self.__name = value
    name = property(_get_name, _set_name)

    # property: user [string]
    #   user to connect to the database
    __user = ''
    def _get_user(self):
        return self.__user
    def _set_user(self, value):
        self.__user = value
    user = property(_get_user, _set_user)

    # property: password [string]
    #   password of user
    __password = ''
    def _get_password(self):
        return self.__password
    def _set_password(self, value):
        self.__password = value
    password = property(_get_password, _set_password)

    # property: mysqlconnection [MySQLdb.Connection]
    #   password of user
    __mysqlconnection = None
    def _get_mysqlconnection(self):
        return self.__mysqlconnection
    mysqlconnection = property(_get_mysqlconnection)

    # property: is_connected [boolean]
    #   indicates if connection is connected
    __is_connected = False
    def _get_is_connected(self):
        return self.__is_connected
    is_connected = property(_get_is_connected)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------

    # method: connect [void]
    #   connects to the db
    def connect(self):
        try:
            self.__mysqlconnection = mysql.connect(self.host, 
                                        self.user, 
                                        self.password, 
                                        self.name, charset='utf8')
            self.__is_connected = True
        except mysql.Error as e:
            self.__is_connected = False
            raise e

    # method: close [void]
    #   wrapper method for closing the connection
    def close(self):
        self.__mysqlconnection.close()
        self.__is_connected = False
        return

    # method: commit [void]
    #   wrapper method for commiting queries
    def commit(self):
        self.__mysqlconnection.commit()

    # method: method rollback [void]
    #   wrapper for rollback queries
    def rollback(self):
        self.__mysqlconnection.rollback()

    # method: cursor [MySQLdb.cursors.Cursor]
    #   wrapper method to create a cursor 
    def cursor(self, cursortype=cursors.DictCursor):
        return self.mysqlconnection.cursor(cursortype)

    # method: createCommand [DbCommand]
    #   creates a DbCommand
    def createCommand(self, sql):
        return DbCommand(self, sql)

    # method: createSelectCommand [DbSelectCommand]
    #   creates a DbSelectCommand
    def createSelectCommand(self, tablename):
        return DbSelectCommand(self, tablename)

    # method: createInsertCommand [DbInsertCommand]
    #   creates a DbInsertCommand
    def createInsertCommand(self, tablename):
        return DbInsertCommand(self, tablename)

    # method: createUpdateCommand [DbUpdateCommand]
    #   creates a DbUpdateCommand
    def createUpdateCommand(self, tablename):
        return DbUpdateCommand(self, tablename)

    # method: createDeleteCommand [DbDeleteCommand]
    #   creates a DbDeleteCommand
    def createDeleteCommand(self, tablename):
        return DbDeleteCommand(self, tablename)
    
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbCommand
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbCommand(object):

    # constructor 
    #   parameters:
    #       dbconnection    a initialized dbconnection
    #                       wich the command uses
    #       sql             either a sqlstatement-object 
    #                       or a sql-string   
    def __init__(self, dbconnection, sql):
        self.__connection = dbconnection
        self.__sql = sql
        self.__parameters = dict()
    
    # ------------------------------------------------------
    #   public properties 
    # ------------------------------------------------------

    # property: connection [DbConnection] (readonly)
    #   holds a reference to the dbconnection object
    __connection = None
    def _get_connection(self):
        return self.__connection
    connection = property(_get_connection)

    # property: sql [SQLStatement or string]
    #   holds a reference to a sqlstatement-object
    #   or a sql-string
    __sql = None
    def _get_sql(self):
        return self.__sql
    def _set_sql(self, value):
        self.__sql = value
    sql = property(_get_sql, _set_sql)

    # property: query [string] (readonly)
    #   returns query string used by command
    def _get_query(self):
        if self.is_sqlstatement:
            query = self.sql.render()
        else:
            query = str(self.sql)
        return query
    query = property(_get_query)

    # property: is_sqlstatement [boolean] (readonly)
    #   indicates if sql-property is a sqlstatement-object
    #   used by a dbcommand-object when executed
    def _get_is_sqlstatement(self):
        return isinstance(self.sql, DbSqlStatement)
    is_sqlstatement = property(_get_is_sqlstatement)

    # property: parameters [dictionary] (readonly)
    #   holds a reference to the parameters collection
    #   used by a dbcommand-object when executed
    __parameters = dict()
    def _get_parameters(self):
        return self.__parameters
    parameters = property(_get_parameters)

    # property: lastrowid [integer] (readonly)
    #   returns last id of affected row by command
    #   if no rows are affected, it will return -1
    __lastrowid = -1
    def _get_lastrowid(self):
        return self.__lastrowid
    lastrowid = property(_get_lastrowid)

    # property: rowsaffected [integer] (readonly)
    #   returns count of rows affected by command
    #   if no rows are affected, it will return -1
    __rowsaffected = -1
    def _get_rowsaffected(self):
        return self.__rowsaffected
    rowsaffected = property(_get_rowsaffected)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------
    
    # method: registerField
    #   registers a field used in query
    def registerField(self, name, value=None):
        self.sql.registerField(name)
        if (value is not None):
            self.parameters[name] = value

    # method: registerCondition
    #   registers a condition used in query
    def registerCondition(self, name, value=None):
        self.sql.registerCondition(name)
        if (value is not None):
            self.parameters[name] = value

    # method: registerCondition
    #   registers a condition used in query
    def registerConditionWithOperand(self, name, operand, value=None):
        self.sql.registerCondition(name, operand)
        if (value is not None):
            self.parameters[name] = value

    # method: registerLikeCondition
    #   registers a condition with like operand used in query
    def registerLikeCondition(self, name, value=None):
        self.registerConditionWithOperand(name, 'LIKE', value)

    # method: registerNotLikeCondition
    #   registers a condition with not like operand used in query
    def registerNotLikeCondition(self, name, value=None):
        self.registerConditionWithOperand(name, 'NOT LIKE', value)

    # method: registerParameter
    #   registers a condition used in query
    def registerParameter(self, name, value):
        self.parameters[name] = value

    # method: fetchone [rows]
    #   executes command and returns one row
    def fetchone(self, cursortype=cursors.DictCursor):
        conn_open = self.connection.is_connected
        if not (conn_open):
            self.connection.connect()
        try:
            cursor = self.connection.cursor(cursortype)
            self._execute(cursor)
            ret = cursor.fetchone()
            cursor.close()
            return ret
        except mysql.Error as e:
            raise e
        except mysql.Warning as e:
            raise e
        finally:
            if not (conn_open):
                self.connection.close()

    # method: fetchall [rows]
    #   executes command and returns rows
    def fetchall(self, cursortype=cursors.DictCursor):
        conn_open = self.connection.is_connected
        if not (conn_open):
            self.connection.connect()
        try:
            self.connection.connect()
            cursor = self.connection.cursor(cursortype)
            self._execute(cursor)
            ret = cursor.fetchall()
            cursor.close()
            return ret
        except mysql.Error as e:
            raise e
        except mysql.Warning as e:
            raise e
        finally:
            if not (conn_open):
                self.connection.close()

    # method: commit [void]
    #   commits the command
    def commit(self):
        conn_open = self.connection.is_connected
        if not (conn_open):
            self.connection.connect()
        try:
            self.connection.connect()
            cursor = self.connection.cursor(cursors.Cursor)
            self._execute(cursor)
            self.connection.commit()
            cursor.close()
        except mysql.Error as e:
            self.connection.rollback()
            raise e
        except mysql.Warning as e:
            self.connection.rollback()
            raise e
        finally:
            if not (conn_open):
                self.connection.close()

    # ------------------------------------------------------
    #   private methods 
    # ------------------------------------------------------

    # method: _execute [void]
    #   registers a condition used in query
    def _execute(self, cursor):

        if isinstance(self.parameters, dict):
            p = tuple(self.parameters.values())
        elif isinstance(self.parameters, list):
            p = tuple(self.parameters)
        else:
            p = self.parameters

        query = self.query
        #print(query)

        if self.parameters is None or len(self.parameters) is 0:
            cursor.execute(query)
        else:
            cursor.execute(query, self.parameters)

        self.__rowsaffected = cursor.rowcount
        self.__lastrowid = cursor.lastrowid

# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbSelectCommand
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbSelectCommand(DbCommand):
    def __init__(self, dbconnection, tablename):
        sql = DbSqlSelectStatement(tablename)
        super(DbSelectCommand, self).__init__(dbconnection, sql)

    def _get_limit_start(self):
        return self.sql.limit_start
    def _set_limit_start(self, value):
        self.sql.limit_start = value
    limit_start = property(_get_limit_start, _set_limit_start)

    def _get_limit_duration(self):
        return self.sql.limit_duration
    def _set_limit_duration(self, value):
        self.sql.limit_duration = value
    limit_duration = property(_get_limit_duration, _set_limit_duration)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------

    # method: registerField
    #   overrides base method, we don't have field-values
    #   in select-queries
    def registerField(self, name):
        super(DbSelectCommand, self).registerField(name)

    def registerRelation(self, field, referencetable, referencefield):
        self.sql.registerRelation(field, referencetable, referencefield)

    def registerRemoteRelation(self, table, field, referencetable, referencefield):
        self.sql.registerRemoteRelation(table, field, referencetable, referencefield)
        
    # method: execute [rows]
    #   executes query and returns all rows
    def execute(self):
        return self.fetchall()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbInsertCommand
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbInsertCommand(DbCommand):
    def __init__(self, dbconnection, tablename):
        sql = DbSqlInsertStatement(tablename)
        super(DbInsertCommand, self).__init__(dbconnection, sql)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------
        
    # method: execute [rows]
    #   executes insert-query and returns lastrowid
    def execute(self):
        self.commit()
        return self.lastrowid

# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbUpdateCommand
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbUpdateCommand(DbCommand):
    def __init__(self, dbconnection, tablename):
        sql = DbSqlUpdateStatement(tablename)
        super(DbUpdateCommand, self).__init__(dbconnection, sql)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------
        
    # method: execute [rows]
    #   executes update-query and returns affected row count
    def execute(self):
        self.commit()
        return self.rowsaffected

# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbDeleteCommand
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbDeleteCommand(DbCommand):
    def __init__(self, dbconnection, tablename):
        sql = DbSqlDeleteStatement(tablename)
        super(DbDeleteCommand, self).__init__(dbconnection, sql)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------

    # method: registerField
    #   overrides base method, we don't have fields
    #   in delete-queries
    def registerField(self, name):
        pass
        
    # method: execute [rows]
    #   executes delete-query and returns affected row count
    def execute(self):
        self.commit()
        return self.rowsaffected

# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbTableAdapter
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbTableAdapter(object):

    def __init__(self, dbconnection, tablename):
        self.__connection = dbconnection
        self.__tablename = tablename
    
    # ------------------------------------------------------
    #   public properties 
    # ------------------------------------------------------

    __connection = None
    def _get_connection(self):
        return self.__connection
    connection = property(_get_connection)

    __tablename = ''
    def _get_tablename(self):
        return self.__tablename
    tablename = property(_get_tablename)

    def count(self):        
        cmd = self.createSelectCommand()
        cmd.registerField('COUNT(*)')
        rows = cmd.execute()
        return int(rows[0]['COUNT(*)'])

    def maxid(self):        
        cmd = self.createSelectCommand()
        cmd.registerField('MAX(id)')
        rows = cmd.execute()
        if (rows[0]['MAX(id)'] is None):
            return 0;
        return int(rows[0]['MAX(id)'])

    # method: createSelectCommand [DbSelectCommand]
    #   wrapper method to create a DbSelectCommand
    def createSelectCommand(self):
        return self.connection.createSelectCommand(self.tablename)

    # method: createInsertCommand [DbInsertCommand]
    #   wrapper method to create a DbInsertCommand
    def createInsertCommand(self):
        return self.connection.createInsertCommand(self.tablename)

    # method: createUpdateCommand [DbUpdateCommand]
    #   wrapper method to create a DbUpdateCommand
    def createUpdateCommand(self):
        return self.connection.createUpdateCommand(self.tablename)

    # method: createDeleteCommand [DbDeleteCommand]
    #   wrapper method to create a DbDeleteCommand
    def createDeleteCommand(self):
        return self.connection.createDeleteCommand(self.tablename)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbFactory
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbFactory(object):

    def __init__(self, config):
        self.host = config.host
        self.database = config.name
        self.user = config.user
        self.password = config.password
    
    # ------------------------------------------------------
    #   public properties 
    # ------------------------------------------------------
    __host = None
    def _get_host(self):
        return self.__host
    def _set_host(self, value):
        self.__host = value
    host = property(_get_host, _set_host)

    __database = None
    def _get_database(self):
        return self.__database
    def _set_database(self, value):
        self.__database = value
    database = property(_get_database, _set_database)

    __user = None
    def _get_user(self):
        return self.__user
    def _set_user(self, value):
        self.__user = value
    user = property(_get_user, _set_user)

    __password = None
    def _get_password(self):
        return self.__password
    def _set_password(self, value):
        self.__password = value
    password = property(_get_password, _set_password)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------
    
    # method: createConnection [DbConnection]
    #   creates a connection
    def createConnection(self):
        conn = DbConnection()
        conn.host = self.host
        conn.name = self.database
        conn.user = self.user        
        conn.password = self.password
        return conn

    # method: createCommand [DbCommand]
    #   creates a command
    def createCommand(self, sql):
        conn = self.createConnection()
        return conn.createCommand(sql)

    # method: createSelectCommand [DbSelectCommand]
    #   creates a select command
    def createSelectCommand(self, tablename):
        conn = self.createConnection()
        return conn.createSelectCommand(tablename)

    # method: createDbInsertCommand [DbInsertCommand]
    #   creates a insert command
    def createInsertCommand(self, tablename):
        conn = self.createConnection()
        return conn.createInsertCommand(tablename)

    # method: createDbUpdateCommand [DbUpdateCommand]
    #   creates a update command
    def createUpdateCommand(self, tablename):
        conn = self.createConnection()
        return conn.createUpdateCommand(tablename)

    # method: createDbDeleteCommand [DbDeleteCommand]
    #   creates a delete command
    def createDeleteCommand(self, tablename):
        conn = self.createConnection()
        return conn.createDeleteCommand(tablename)
