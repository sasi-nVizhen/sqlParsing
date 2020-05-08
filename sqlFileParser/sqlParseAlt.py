import ply.lex as lex, re

tokens = (
    "TABLE",
    "JOIN",
    "COLUMN",
    "TRASH"
)

tables = {"tables": {}, "alias": {}}
columns = []

t_TRASH = r"Select|on|=|;|\s+|,|\t|\r"

def t_TABLE(t):
    r"from\s(\w+)\sas\s(\w+)"

    regex = re.compile(t_TABLE.__doc__)
    m = regex.search(t.value)
    if m is not None:
        tbl = m.group(1)
        alias = m.group(2)
        tables["tables"][tbl] = ""
        tables["alias"][alias] = tbl

    return t

def t_JOIN(t):
    r"inner\s+join\s+(\w+)\s+as\s+(\w+)"

    regex = re.compile(t_JOIN.__doc__)
    m = regex.search(t.value)
    if m is not None:
        tbl = m.group(1)
        alias = m.group(2)
        tables["tables"][tbl] = ""
        tables["alias"][alias] = tbl
    return t

def t_COLUMN(t):
    r"(\w+\.\w+)"

    regex = re.compile(t_COLUMN.__doc__)
    m = regex.search(t.value)
    if m is not None:
        t.value = m.group(1)
        columns.append(t.value)
    return t

def t_error(t):
    raise TypeError("Unknown text '%s'" % (t.value,))
    t.lexer.skip(len(t.value))

# here is where the magic starts
def mylex(inp):
    lexer = lex.lex()
    lexer.input(inp)

    for token in lexer:
        pass

    result = {}
    for col in columns:
        tbl, c = col.split('.')
        if tbl in tables["alias"].keys():
            key = tables["alias"][tbl]
        else:
            key = tbl

        if key in result:
            result[key].append(c)
        else:
            result[key] = list()
            result[key].append(c)

    print(result)
    # {'tb1': ['col1', 'col7'], 'tb2': ['col2', 'col8']}
with open('test.sql', 'r') as file:
    string = file.read().replace('\n', ' ')
#string = "Select a.col1, b.col2 from tb1 as a inner join tb2 as b on tb1.col7 = tb2.col8;"
mylex(string)