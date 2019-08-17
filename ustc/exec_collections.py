import subprocess
from shlex import quote

def csv():
    command = 'mongoexport -u econ -p crazyhorsemonument --db ustc --collection {0} --type=csv --fieldFile fields/all_fields.csv --out ./csvs/{0}2.csv'
    # command = 'mongoexport --db ustc --collection {0} --type=csv --fieldFile fields/all_fields.csv --out ./csvs/{0}.csv'
    with open('collections.csv') as fd:
        collections = fd.readline().split(',')
        collections = ['Military_Handbooks']
        for c in collections:
            c = command.format(quote(c).strip(' \n'))
            print(c)
            subprocess.run(c, shell=True)

csv()

def fields():
    #  mongo -u econ -p crazyhorsemonument ustc --eval "var collection = 'Medical_Texts'" variety.js
    # command = 'mongo ustc --eval "var collection = \'{}\'" variety.js > {}.txt'
    command = 'mongo -u econ -p crazyhorsemonument ustc --eval "var collection = \'{0}\'" variety.js > ../{0}.txt'
    with open('collections.csv') as fd:
        collections = fd.readline().split(',')
        for c in collections:
            c = command.format(c.strip(' \n'))
            subprocess.run(c, shell=True, cwd='variety')
