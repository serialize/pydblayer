#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import getopt
import warnings
import string

def enum(*sequential, **values):
    enums = dict(zip(sequential, range(len(sequential))), **values)
    reverse = dict((value, key) for key, value in enums.iteritems())
    enums["reverse_mapping"] = reverse
    return type('Enum', (), enums)

EN_KEYTYPE = enum(No = 0,
                  Primary = 1, 
                  Foreign = 2)

EN_ENCLOSEMENT = enum(No = 0,
                      Square = 1,
                      Round = 2,
                      Curly = 3,
                      Quote = 4,
                      DoubleQuote = 5)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbSqlTokenizer
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbSqlTokenizer(object):


    def __init__(self, separator = '', tokens = None):
        self.__tokens = list()
        self.__separator = separator

        if tokens is not None:
            self.append(tokens)

    def __del__(self):
        pass
    
    # ------------------------------------------------------
    #   public properties 
    # ------------------------------------------------------
    
    def _get_count(self):
        return len(self.__tokens)
    count = property(_get_count)

    __tokens = list()
    def _get_tokens(self):
        return self.__tokens
    tokens = property(_get_tokens)

    __separator = ""
    def _get_separator(self):
        return self.__separator
    separator = property(_get_separator)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------

    def append(self, obj):
        if isinstance(obj, (list, tuple)):
            self.tokens.extend(obj)
        elif isinstance(obj, dict):
            self.tokens.extend(obj.values())
        elif isinstance(obj, DbSqlTokenizer):
            self.tokens.append(obj.render())
        else:
            self.tokens.append(str(obj))
        return

    def render(self, separator = None):
        sep = str(self.separator)
        if separator is not None:
            sep = str(separator)
        
        temp = list(self.tokens)
        queue = list()
        for t in temp:
            if isinstance(t, DbSqlTokenizer):
                queue.append(t.render())
            else:
                queue.append(t)

        self.__tokens = list()
        ret = sep.join(queue)
        del queue
        return ret


# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbSqlExpression
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbSqlExpression(DbSqlTokenizer):

    def __init__(self, separator = ' '):
        super(DbSqlExpression, self).__init__(separator)
        self.__enclosement = EN_ENCLOSEMENT.No
    
    # ------------------------------------------------------
    #   public properties 
    # ------------------------------------------------------
    
    __enclosement = None
    def _get_enclosement(self):
        return self.__enclosement
    def _set_enclosement(self, value):
        self.__enclosement = value
    enclosement = property(_get_enclosement, _set_enclosement)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------

    def render(self):

        if self.enclosement is None:
            return super(DbSqlExpression, self).render()

        enc = []
        if self.enclosement == EN_ENCLOSEMENT.Square:
            enc = ["[", "]"]
        elif self.enclosement == EN_ENCLOSEMENT.Round:
            enc = ["(", ")"]
        elif self.enclosement == EN_ENCLOSEMENT.Curly:
            enc = ["{", "}"]
        elif self.enclosement == EN_ENCLOSEMENT.Quote:
            enc = ["'", "'"]
        elif self.enclosement == EN_ENCLOSEMENT.DoubleQuote:
            enc = ['"', '"']
        else: 
            enc = ["", ""]

        return enc[0] + super(DbSqlExpression, self).render() + enc[1]


# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbSqlTableDefinition
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbSqlTableDefinition(object):

    def __init__(self, name):
        self.__name = name
        self.__fields = []
    
    # ------------------------------------------------------
    #   public properties 
    # ------------------------------------------------------

    __name = ''
    def _get_name(self):
        return self.__name
    def _set_name(self, value):
        self.__name = value
    name = property(_get_name, _set_name)

    __fields = []
    def _get_fields(self):
        return self.__fields
    fields = property(_get_fields)
    
    def _get_fieldLength(self):
        return len(self.__fields)
    fieldLength = property(_get_fieldLength)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------

    def clearFields(self):
        self.__fields.clear()

    def registerField(self, name):
        field = DbSqlFieldDefinition(self, name)
        self.__fields.append(field)
        return field;

    def existField(self, name):
        for field in self.fields:
            if field.name == name:
                return True
        return False


# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbSqlFieldDefinition
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbSqlFieldDefinition(object):

    def __init__(self, table, name):
        self.__name = name
        self.__table = table
    
    # ------------------------------------------------------
    #   public properties 
    # ------------------------------------------------------
    
    __table = None
    def _get_table(self):
        return self.__table
    table = property(_get_table)

    __name = ''
    def _get_name(self):
        return self.__name
    def _set_name(self, value):
        self.__name = value
    name = property(_get_name, _set_name)

    __sqltype = ''
    def _get_sqltype(self):
        return self.__sqltype
    def _set_sqltype(self, value):
        self.__sqltype = value
    sqltype = property(_get_sqltype, _set_sqltype)

    __length = ''
    def _get_length(self):
        return self.__length
    def _set_length(self, value):
        self.__length = value
    length = property(_get_length, _set_length)

    __display = ''
    def _get_display(self):
        return self.__display
    def _set_display(self, value):
        self.__display = value
    display = property(_get_display, _set_display)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbSqlCondition
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbSqlCondition(DbSqlExpression):

    def __init__(self, field, comparsion = '=', operator = 'AND'):
        super(DbSqlCondition, self).__init__(' ')
        self.__field = field
        self.__comparsion = comparsion
        self.__operator = operator
    
    # ------------------------------------------------------
    #   public properties 
    # ------------------------------------------------------

    __field = ''
    def _get_field(self):
        return self.__field
    def _set_field(self, value):
        self.__field = value
    field = property(_get_field, _set_field)

    __comparsion = ''
    def _get_comparsion(self):
        return self.__comparsion
    def _set_comparsion(self, value):
        self.__comparsion = value
    comparsion = property(_get_comparsion, _set_comparsion)

    __operator = ''
    def _get_operator(self):
        return self.__operator
    def _set_operator(self, value):
        self.__operator = value
    operator = property(_get_operator, _set_operator)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------
    
    def render(self):
        self.append(self.__field)
        self.append(self.__comparsion)
        self.append('%(' + self.__field + ')s')

        tokens = DbSqlTokenizer(' ')
        tokens.append(self.__operator)
        tokens.append(super(DbSqlCondition, self).render())
        return tokens.render()
        
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbSqlJoin
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbSqlJoin(DbSqlExpression):

    def __init__(self, table, field, referencetable, referencefield):
        super(DbSqlJoin, self).__init__(' ')
        self.__table = table
        self.__field = field
        self.__referencetable = referencetable
        self.__referencefield = referencefield
    
    # ------------------------------------------------------
    #   public properties 
    # ------------------------------------------------------

    __table = ''
    def _get_table(self):
        return self.__table
    def _set_table(self, value):
        self.__table = value
    table = property(_get_table, _set_table)

    __field = ''
    def _get_field(self):
        return self.__field
    def _set_field(self, value):
        self.__field = value
    field = property(_get_field, _set_field)

    __referencetable = ''
    def _get_referencetable(self):
        return self.__referencetable
    def _set_referencetable(self, value):
        self.__referencetable = value
    referencetable = property(_get_referencetable, _set_referencetable)

    __referencefield = ''
    def _get_referencefield(self):
        return self.__referencefield
    def _set_referencefield(self, value):
        self.__referencefield = value
    referencefield = property(_get_referencefield, _set_referencefield)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------
    
    def render(self):
        self.append('JOIN')
        self.append(self.__referencetable)
        self.append('ON')
        self.append(self.__table + '.' + self.__field)
        self.append('=')
        self.append(self.__referencetable + '.' + self.__referencefield)
        return super(DbSqlJoin, self).render()
        
        
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbSqlInnerJoin
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbSqlInnerJoin(DbSqlJoin):

    def __init__(self, table, field, referencetable, referencefield):
        super(DbSqlInnerJoin, self).__init__(table, field, referencetable, referencefield)
    
    def render(self):
        self.append('INNER')
        return super(DbSqlInnerJoin, self).render()



# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbSqlStatement
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbSqlStatement(DbSqlExpression):

    def __init__(self, table):
        self.__table = table
        self.__fields = []
        self.__relations = []
        self.__conditions = []
        super(DbSqlStatement, self).__init__(' ')
    
    # ------------------------------------------------------
    #   public properties 
    # ------------------------------------------------------
    
    __table = ''
    def _get_table(self):
        return self.__table
    table = property(_get_table)
    
    __fields = []
    def _get_fields(self):
        return self.__fields
    fields = property(_get_fields)
    
    def _get_fieldLength(self):
        return len(self.__fields)
    fieldLength = property(_get_fieldLength)
    
    __conditions = []
    def _get_conditions(self):
        return self.__conditions
    conditions = property(_get_conditions)
    
    def _get_conditionLength(self):
        return len(self.__conditions)
    conditionLength = property(_get_conditionLength)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------

    def appendTable(self):
        self.append(self.__table)

    def appendFieldsDirect(self, enclosed = False):
        self._appendFields("%v", enclosed)

    def appendFieldsAsAssignment(self, enclosed = False):
        self._appendFields("%v = %(%v)s", enclosed)

    def appendFieldsAsParameter(self, enclosed = False):
        self._appendFields("%(%v)s", enclosed)

    def appendConditions(self):
        for condition in self.__conditions:
            self.append(condition)

    def appendRelations(self):
        for relation in self.__relations:
            self.append(relation)

    def registerField(self, name):
        self.__fields.append(name)

    def registerRelation(self, field, referencetable, referencefield):
        self.__relations.append(DbSqlInnerJoin(self.__table, field, referencetable, referencefield))

    def registerRemoteRelation(self, table, field, referencetable, referencefield):
        self.__relations.append(DbSqlInnerJoin(table, field, referencetable, referencefield))

    def registerCondition(self, field, comparsion = '=', operator = 'AND', enclosed = False):
        condition = None
        if self.conditionLength == 0:
            condition = DbSqlCondition(field, comparsion, 'WHERE')
        else:
            condition = DbSqlCondition(field, comparsion, operator)
        if enclosed:
            condition.enclosement = EN_ENCLOSEMENT.Round
        self.__conditions.append(condition)

    def registerStringCondition(self, field, operator = 'AND'):
        self.registerCondition(field, 'LIKE', operator)
    def render(self):
        return super(DbSqlStatement, self).render()

    # ------------------------------------------------------
    #   private methods 
    # ------------------------------------------------------

    def _appendFields(self, template, enclosed = False):
        tokenizer = DbSqlExpression(', ')
        if enclosed:
            tokenizer.enclosement = EN_ENCLOSEMENT.Round
        for field in self.__fields:
            tokenizer.append(str(template).replace("%v", field))
        self.append(tokenizer)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbSqlSelectStatement
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbSqlSelectStatement(DbSqlStatement):

    def __init__(self, table):
        super(DbSqlSelectStatement, self).__init__(table)
    
    # ------------------------------------------------------
    #   public properties 
    # ------------------------------------------------------

    __limit_start = 0
    def _get_limit_start(self):
        return self.__limit_start
    def _set_limit_start(self, value):
        self.__limit_start = value
    limit_start = property(_get_limit_start, _set_limit_start)

    __limit_duration = 0
    def _get_limit_duration(self):
        return self.__limit_duration
    def _set_limit_duration(self, value):
        self.__limit_duration = value
    limit_duration = property(_get_limit_duration, _set_limit_duration)

    __distinct = False
    def _get_distinct(self):
        return self.__distinct
    def _set_distinct(self, value):
        self.__distinct = value
    distinct = property(_get_distinct, _set_distinct)

    __orderby = ''
    def _get_orderby(self):
        return self.__orderby
    def _set_orderby(self, value):
        self.__orderby = value
    orderby = property(_get_orderby, _set_orderby)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------

    def render(self):

        self.append("SELECT")
        if self.distinct:
            self.append("distinct")
        self.appendFieldsDirect()
        self.append("FROM")
        self.appendTable()
        self.appendRelations()
        self.appendConditions()
        if self.limit_duration > 0:
            self.append("LIMIT " + str(self.limit_start) + ", " + str(self.limit_duration))
        if self.orderby != '':
            self.append('ORDER BY ' + self.orderby)
        return super(DbSqlSelectStatement, self).render()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbSqlInsertStatement
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbSqlInsertStatement(DbSqlStatement):

    def __init__(self, table):
        super(DbSqlInsertStatement, self).__init__(table)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------

    def render(self):

        self.append("INSERT INTO")
        self.appendTable()
        self.appendFieldsDirect(True)
        self.append("VALUES")
        self.appendFieldsAsParameter(True)

        return super(DbSqlInsertStatement, self).render()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbSqlUpdateStatement
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbSqlUpdateStatement(DbSqlStatement):

    def __init__(self, table):
        super(DbSqlUpdateStatement, self).__init__(table)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------

    def render(self):

        self.append("UPDATE")
        self.appendTable()
        self.append("SET")
        self.appendFieldsAsAssignment()
        self.appendConditions()

        return super(DbSqlUpdateStatement, self).render()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# class DbSqlDeleteStatement
#
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
class DbSqlDeleteStatement(DbSqlStatement):

    def __init__(self, table):
        super(DbSqlDeleteStatement, self).__init__(table)

    # ------------------------------------------------------
    #   public methods 
    # ------------------------------------------------------

    def render(self):

        self.append("DELETE")
        self.append("FROM")
        self.appendTable()
        self.appendConditions()

        return super(DbSqlDeleteStatement, self).render()

