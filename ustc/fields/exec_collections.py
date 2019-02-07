import subprocess
#  mongo -u econ -p crazyhorsemonument ustc --eval "var collection = 'Medical_Texts'" variety.js
# command = 'mongo ustc --eval "var collection = \'{}\'" variety.js > {}.txt'
command = 'mongo -u econ -p crazyhorsemonument ustc --eval "var collection = \'{0}\'" variety.js > ../{0}.txt'
with open('collections.csv') as fd:
    collections = fd.readline().split(',')
    for c in collections:
        c = command.format(c.strip(' \n'))
        subprocess.run(c, shell=True, cwd='variety')
