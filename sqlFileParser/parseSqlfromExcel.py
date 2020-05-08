import xlrd
import re
import pandas as pd
import itertools
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                value = identifier.value.replace('"', '').lower()
                yield value
        elif isinstance(item, Identifier):
            value = item.value.replace('"', '').lower()
            yield value


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
        # if item.is_group:
        #    for x in extract_from_part(item):
        #        yield x
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword: # and item.value.upper() in ['ORDER', 'GROUP', 'BY', 'HAVING', 'GROUP BY']:
                return
            else:
                yield item

        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True
def extract_into_part(parsed):
    into_seen = False
    for item in parsed.tokens:
        if into_seen:
            if is_subselect(item):
                for x in extract_into_part(item):
                    yield x
            elif item.ttype is Keyword:
                return
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'INTO':
            into_seen = True

def extract_join_part(parsed):
    join_seen = False
    for item in parsed.tokens:
        if join_seen:
            if is_subselect(item):
                for x in extract_join_part(item):
                    yield x
            elif item.ttype is Keyword:
                return
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'INNER JOIN':
            join_seen = True

def extract_tables(sql):
    # let's handle multiple statements in one sql string

    extracted_tables = []
    statements = (sqlparse.parse(sql))
    for statement in statements:
        if statement.get_type() != 'UNKNOWN':
            stream = extract_from_part(statement)
            extracted_tables.append(set(list(extract_table_identifiers(stream))))
            into_stream = extract_into_part(statement)
            extracted_tables.append(set(list(extract_table_identifiers(into_stream))))
            join_stream = extract_join_part(statement)
            extracted_tables.append(set(list(extract_table_identifiers(join_stream))))

    return list(itertools.chain(*extracted_tables))

if __name__ == '__main__':
    # location of the excel file
    loc = "SQLStmt.xlsx"
    # Open the workbook
    wb = xlrd.open_workbook(loc, 'r')
    sheet = wb.sheet_by_index(0)
    for row_idx in range(sheet.nrows):
        for col_idx in range(sheet.ncols):
            # parse_query(sheet.cell(row_idx, col_idx).value)
            tables = ', '.join(extract_tables(sheet.cell(row_idx, col_idx).value))
            print('Tables:{0}'.format(tables))


