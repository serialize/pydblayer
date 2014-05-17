#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import unittest
import MySQLdb as mysql
import RIP_Common
from RIP_Common import *
from TEST_Utils import *
from RIP_DbSqlCompiler import *
from RIP_DbAccess import *

class TEST_DbConnection(unittest.TestCase):

    # test if inpit params are assigned to properties
    def test_dbconnection_01_initialization(self):
        cfg = RipDatabaseConfig()
        cfg.host = 'test_host'
        cfg.name = 'test_name'
        cfg.user = 'test_user'
        cfg.password = 'test_pw'
        db = DbConnection(cfg)
        self.assertEqual(db.host, 'test_host')
        self.assertEqual(db.name, 'test_name')
        self.assertEqual(db.user, 'test_user')
        self.assertEqual(db.password, 'test_pw')

    # test connect to a database 
    def test_dbconnection_02_connect(self):
        db = DbConnection(RIP_Common.get_database_config())
        db.connect()
        self.assertEqual(db.is_connected, True)
        db.close()

    # test cursor creation
    def test_dbconnection_03_create_cursor(self):
        db = DbConnection(RIP_Common.get_database_config())
        db.connect()
        cur = db.cursor(mysql.cursors.Cursor)
        self.assertTrue(isinstance(cur, mysql.cursors.Cursor))
        cur = db.cursor()
        self.assertTrue(isinstance(cur, mysql.cursors.DictCursor))
        db.close()



class TEST_DbCommand(unittest.TestCase):

    def test_dbcommand_01_validate_type(self):
        db = DbConnection(RIP_Common.get_database_config())
        db.connect()
        cmd = db.createCommand('SELECT VERSION()')
        self.assertTrue(isinstance(cmd, DbCommand))
        cmd = db.createSelectCommand('table')
        self.assertTrue(isinstance(cmd, DbSelectCommand))
        cmd = db.createInsertCommand('table')
        self.assertTrue(isinstance(cmd, DbInsertCommand))
        cmd = db.createUpdateCommand('table')
        self.assertTrue(isinstance(cmd, DbUpdateCommand))
        cmd = db.createDeleteCommand('table')
        self.assertTrue(isinstance(cmd, DbDeleteCommand))
        db.close()

    def test_dbcommand_02_execute_setup(self):
        db = DbConnection(RIP_Common.get_database_config())
        db.connect()
        cur = db.cursor(cursors.Cursor)
        cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_schema LIKE '" + db.name + "' AND table_name LIKE 'UnitTests'")
        res = cur.fetchone() 
        if (res[0] > 0):
            cur.execute("DROP TABLE UnitTests;")
        db.commit()
        cur.execute('CREATE TABLE UnitTests (Id INT PRIMARY KEY AUTO_INCREMENT, TestName VARCHAR(25), TestName2 VARCHAR(25) NULL);')
        cur.execute("INSERT INTO UnitTests (TestName, TestName2) VALUES ('testdata1', 'test1');")
        cur.execute("INSERT INTO UnitTests (TestName, TestName2) VALUES ('testdata2', 'test2');")
        cur.execute("INSERT INTO UnitTests (TestName, TestName2) VALUES ('testdata3', 'test3');")
        db.commit()
        cur.execute("SELECT COUNT(*) FROM UnitTests")
        res = cur.fetchone() 
        cur.close()
        db.close()
        self.assertEqual(res[0], 3)
        
    def test_dbcommand_03_select_testdata(self):
        db = DbConnection(RIP_Common.get_database_config())
        cmd = db.createSelectCommand('UnitTests')
        cmd.registerField('TestName')
        rows = cmd.execute()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]['TestName'], 'testdata1')
        self.assertEqual(rows[1]['TestName'], 'testdata2')
        self.assertEqual(rows[2]['TestName'], 'testdata3')

    def test_dbcommand_04_insert_testdata(self):
        db = DbConnection(RIP_Common.get_database_config())
        cmd = db.createInsertCommand('UnitTests')
        cmd.registerField('TestName', 'testdata4insert')
        res = cmd.execute()
        self.assertEqual(int(res), 4)
        cmd = db.createSelectCommand('UnitTests')
        cmd.registerField('TestName')
        rows = cmd.execute()
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]['TestName'], 'testdata1')
        self.assertEqual(rows[1]['TestName'], 'testdata2')
        self.assertEqual(rows[2]['TestName'], 'testdata3')
        self.assertEqual(rows[3]['TestName'], 'testdata4insert')

    def test_dbcommand_05_update_testdata_id(self):
        db = DbConnection(RIP_Common.get_database_config())
        cmd = db.createUpdateCommand('UnitTests')
        cmd.registerField('TestName', 'testdata2update')
        cmd.registerCondition('id', 2)
        res = cmd.execute()
        self.assertEqual(int(res), 1)
        cmd = db.createSelectCommand('UnitTests')
        cmd.registerField('TestName')
        rows = cmd.execute()
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]['TestName'], 'testdata1')
        self.assertEqual(rows[1]['TestName'], 'testdata2update')
        self.assertEqual(rows[2]['TestName'], 'testdata3')
        self.assertEqual(rows[3]['TestName'], 'testdata4insert')

    def test_dbcommand_06_update_testdata_name(self):
        db = DbConnection(RIP_Common.get_database_config())
        cmd = db.createUpdateCommand('UnitTests')
        cmd.registerField('TestName', 'testdata1update')
        cmd.registerCondition('TestName2', 'test1')
        res = cmd.execute()
        self.assertEqual(int(res), 1)
        cmd = db.createSelectCommand('UnitTests')
        cmd.registerField('TestName')
        rows = cmd.execute()
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]['TestName'], 'testdata1update')
        self.assertEqual(rows[1]['TestName'], 'testdata2update')
        self.assertEqual(rows[2]['TestName'], 'testdata3')
        self.assertEqual(rows[3]['TestName'], 'testdata4insert')

    def test_dbcommand_07_delete_testdata(self):
        db = DbConnection(RIP_Common.get_database_config())
        cmd = db.createDeleteCommand('UnitTests')
        cmd.registerCondition('id', 3)
        res = cmd.execute()
        self.assertEqual(int(res), 1)
        cmd = db.createSelectCommand('UnitTests')
        cmd.registerField('TestName')
        rows = cmd.execute()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]['TestName'], 'testdata1update')
        self.assertEqual(rows[1]['TestName'], 'testdata2update')
        self.assertEqual(rows[2]['TestName'], 'testdata4insert')

    def test_dbcommand_08_drop_testtable(self):
        db = DbConnection(RIP_Common.get_database_config())
        db.connect()
        cur = db.cursor(cursors.Cursor)
        cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_schema LIKE '" + db.name + "' AND table_name LIKE 'UnitTests'")
        res = cur.fetchone() 
        if (res[0] > 0):
            cur.execute("DROP TABLE UnitTests;")
        db.commit()
        cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_schema LIKE '" + db.name + "' AND table_name LIKE 'UnitTests'")
        res = cur.fetchone() 
        cur.close()
        db.close()
        self.assertEqual(res[0], 0)
