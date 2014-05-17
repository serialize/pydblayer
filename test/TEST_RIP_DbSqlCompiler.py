#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import unittest
from TEST_Utils import *
from RIP_DbSqlCompiler import *

class TEST_DbSqlTokenizer(unittest.TestCase):

	def setUp(self):
		pass

	def test_sqltokenizer_initwithoutparams(self):
		s = DbSqlTokenizer()
		s.append(1)
		s.append(2)
		s.append(3)
		self.assertEqual("123", s.render())

	def test_sqltokenizer_initwithseparator(self):
		s = DbSqlTokenizer("-")
		s.append(1)
		s.append(2)
		s.append(3)
		self.assertEqual("1-2-3", s.render())

class TEST_DbSqlExpression(unittest.TestCase):

	def setUp(self):
		pass

	def test_sqlexpression_initdefault(self):
		s = DbSqlExpression()
		s.append(1)
		s.append(2)
		s.append(3)
		self.assertEqual("1 2 3", s.render())

	def test_sqlexpression_initenclosement(self):
		s = DbSqlExpression(", ")
		s.enclosement = EN_ENCLOSEMENT.Round
		s.append(1)
		s.append(2)
		s.append(3)
		self.assertEqual("(1, 2, 3)", s.render())

class TEST_DbSqlCondition(unittest.TestCase):

	def setUp(self):
		pass

	def test_sqlcondition_init_1parameters(self):
		s = DbSqlCondition('f1')
		self.assertEqual("AND f1 = %(f1)s", s.render())

	def test_sqlcondition_init_2parameters(self):
		s = DbSqlCondition('f1', 'LIKE')
		self.assertEqual("AND f1 LIKE %(f1)s", s.render())

	def test_sqlcondition_init_3parameters(self):
		s = DbSqlCondition('f1', 'LIKE', 'WHERE')
		self.assertEqual("WHERE f1 LIKE %(f1)s", s.render())

	def test_sqlcondition_enclosement(self):
		s = DbSqlCondition('f1', 'LIKE', 'WHERE')
		s.enclosement = EN_ENCLOSEMENT.Round
		self.assertEqual("WHERE (f1 LIKE %(f1)s)", s.render())

	def test_sqlcondition_property_field(self):
		s = DbSqlCondition('f1')
		s.field = 'f2'
		self.assertEqual("AND f2 = %(f2)s", s.render())

	def test_sqlcondition_property_comparsion(self):
		s = DbSqlCondition('f1')
		s.comparsion = 'LIKE'
		self.assertEqual("AND f1 LIKE %(f1)s", s.render())

	def test_sqlcondition_property_operator(self):
		s = DbSqlCondition('f1')
		s.operator = 'OR'
		self.assertEqual("OR f1 = %(f1)s", s.render())

class TEST_DbSqlStatement(unittest.TestCase):

	def setUp(self):
		pass

	def test_sqlstatement_table(self):
		s = DbSqlStatement('table')
		s.appendTable()
		self.assertEqual('table', s.render())

	def test_sqlstatement_fieldlength(self):
		s = DbSqlStatement('table')
		self.assertEqual(0, s.fieldLength)
		s.registerField('f1')
		s.registerField('f2')
		self.assertEqual(2, s.fieldLength)

	def test_sqlstatement_field_accessbyindex(self):
		s = DbSqlStatement('table')
		s.registerField('f1')
		s.registerField('f2')
		self.assertEqual('f2', s.fields[1])

	def test_sqlstatement_conditionlength(self):
		s = DbSqlStatement('table')
		self.assertEqual(0, s.conditionLength)
		s.registerCondition('c1')
		s.registerCondition('c2')
		s.registerCondition('c3')
		self.assertEqual(3, s.conditionLength)

	def test_sqlstatement_condition_accessbyindex(self):
		s = DbSqlStatement('table')
		s.registerCondition('c1')
		s.registerCondition('c2')
		s.registerCondition('c3')
		self.assertEqual('c3', s.conditions[2].field)

	def test_sqlstatement_fields_append(self):
		s = DbSqlStatement('table')
		s.registerField('f1')
		s.appendFieldsDirect()
		self.assertEqual('f1', s.render())

	def test_sqlstatement_fields_appendassignment(self):
		s = DbSqlStatement('table')
		s.registerField('f1')
		s.registerField('f2')
		s.appendFieldsAsAssignment()
		self.assertEqual('f1 = %(f1)s, f2 = %(f2)s', s.render())

	def test_sqlstatement_fields_appendparams(self):
		s = DbSqlStatement('table')
		s.registerField('f1')
		s.registerField('f2')
		s.registerField('f3')
		s.appendFieldsAsParameter()
		self.assertEqual('%(f1)s, %(f2)s, %(f3)s', s.render())


class TEST_DbSqlJoin(unittest.TestCase):

	def setUp(self):
		pass

	def test_sqlinnerjoin_relation(self):
		s = DbSqlJoin('table1', 'pk', 'table2', 'fk')
		self.assertEqual("JOIN table2 ON table1.pk = table2.fk", s.render())

class TEST_DbSqlInnerJoin(unittest.TestCase):

	def setUp(self):
		pass

	def test_sqlinnerjoin_relation(self):
		s = DbSqlInnerJoin('table1', 'pk', 'table2', 'fk')
		self.assertEqual("INNER JOIN table2 ON table1.pk = table2.fk", s.render())


class TEST_DbSqlSelectStatement(unittest.TestCase):

	def setUp(self):
		pass

	def test_sqlselect_onefield(self):
		s = DbSqlSelectStatement('table')
		s.registerField("field1")
		self.assertEqual("SELECT field1 FROM table", s.render())

	def test_sqlselect_twofields(self):
		s = DbSqlSelectStatement('table')
		s.registerField("field1")
		s.registerField("field2")
		self.assertEqual("SELECT field1, field2 FROM table", s.render())

	def test_sqlselect_distinct(self):
		s = DbSqlSelectStatement('table')
		s.distinct = True
		s.registerField("field1")
		s.registerField("field2")
		self.assertEqual("SELECT distinct field1, field2 FROM table", s.render())

class TEST_DbSqlSelectInnerJoinStatement(unittest.TestCase):

	def setUp(self):
		pass

	def test_sqlinnerjoin_relation(self):
		s = DbSqlSelectStatement('table1')
		s.registerField("pk")
		s.registerField("field2")
		s.registerRelation('pk', 'table2', 'fk')
		self.assertEqual("SELECT pk, field2 FROM table1 INNER JOIN table2 ON table1.pk = table2.fk", s.render())

	def test_sqlinnerjoin_remoterelation(self):
		s = DbSqlSelectStatement('table1')
		s.registerField("pk")
		s.registerField("field2")
		s.registerRelation('pk', 'table2', 'fk')
		s.registerRemoteRelation('table2', 'pk', 'table3', 'fk')
		self.assertEqual("SELECT pk, field2 FROM table1 INNER JOIN table2 ON table1.pk = table2.fk INNER JOIN table3 ON table2.pk = table3.fk", s.render())

class TEST_DbSqlInsertStatement(unittest.TestCase):

	def setUp(self):
		pass

	def test_sqlinsert_onefield(self):
		s = DbSqlInsertStatement('table')
		s.registerField("field1")
		self.assertEqual("INSERT INTO table (field1) VALUES (%(field1)s)", s.render())

	def test_sqlinsert_twofields(self):
		s = DbSqlInsertStatement('table')
		s.registerField("field1")
		s.registerField("field2")
		self.assertEqual("INSERT INTO table (field1, field2) VALUES (%(field1)s, %(field2)s)", s.render())

class TEST_DbSqlUpdateStatement(unittest.TestCase):

	def setUp(self):
		pass

	def test_sqlupdate_onefield(self):
		s = DbSqlUpdateStatement('table')
		s.registerField("field1")
		self.assertEqual("UPDATE table SET field1 = %(field1)s", s.render())

	def test_sqlupdate_twofields(self):
		s = DbSqlUpdateStatement('table')
		s.registerField("field1")
		s.registerField("field2")
		self.assertEqual("UPDATE table SET field1 = %(field1)s, field2 = %(field2)s", s.render())

class TEST_DbSqlDeleteStatement(unittest.TestCase):

	def setUp(self):
		pass

	def test_sqldelete_simple(self):
		s = DbSqlDeleteStatement('table')
		self.assertEqual("DELETE FROM table", s.render())

class DbSqlCompilerSuiteAdapter(TestSuiteAdapter):
	
	def registerAll(self):
		self.registerCase(TEST_DbSqlTokenizer)
		self.registerCase(TEST_DbSqlExpression)
		self.registerCase(TEST_DbSqlCondition)
		self.registerCase(TEST_DbSqlStatement)
		self.registerCase(TEST_DbSqlJoin)
		self.registerCase(TEST_DbSqlInnerJoin)
		self.registerCase(TEST_DbSqlSelectStatement)
		self.registerCase(TEST_DbSqlSelectInnerJoinStatement)
		self.registerCase(TEST_DbSqlInsertStatement)
		self.registerCase(TEST_DbSqlUpdateStatement)
		self.registerCase(TEST_DbSqlDeleteStatement)

if __name__ == "__main__":
	adapter = DbSqlCompilerSuiteAdapter(2)
	adapter.registerAll()
	adapter.runText()


