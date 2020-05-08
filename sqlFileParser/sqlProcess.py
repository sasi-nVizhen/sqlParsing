with open('test.sql', 'r') as file:
    query = file.read().replace('\n', ' ')
print(query)
replace_list = ['\n', '(', ')', '*', '=']
for i in replace_list:
    query = query.replace(i, ' ')
query = query.split()
res = []
for i in range(0, len(query)):
    print(i)
    if query[i-1] in ['from', 'join', 'into', 'union', 'all'] and query[i] != 'select':
        res.append('row-' + str(i) + '-' + query[i-1] + '-' + query[i])
print(res)