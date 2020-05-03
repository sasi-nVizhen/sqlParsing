import re
import pandas as pd 
import itertools
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML



def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword:
                return
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True

def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_name()
        elif isinstance(item, Identifier):
            yield item.get_name()
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            yield item.value           

def extract_tables(sql):
    stream = extract_from_part(sqlparse.parse(sql)[0])
    return list(extract_table_identifiers(stream))

if __name__ == '__main__':
	query = """
	select
    	id,fname,lname,address
	from
    	res_users as r
    left join
        res_partner as p
    on
        p.id=r.partner_id
	Where
    	name = (select name from res_partner where id = 1)"""


tables = ', '.join(extract_tables(sql))
    print('Tables: {0}'.format(tables))












#where = next(token for token in query_tokens.tokens if isinstance(token, Where))
#condition = next(token for token in where.tokens if isinstance(token, Comparison))
#subquery = next(token for token in condition.tokens if isinstance(token, Parenthesis))
#print (subquery)
