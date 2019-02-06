import os

fields = set()
for f in list(filter(lambda x: '.txt' in x, os.listdir())):
    print(f)
    with open(f) as fd:
        lines = fd.readlines()[3:-1]
        for line in lines:
            print(line)
            fields.add(line.split('|')[1].strip())

print(fields)
