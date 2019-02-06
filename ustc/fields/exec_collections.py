import subprocess
#  mongo -u econ -p crazyhorsemonument ustc --eval "var collection = 'Medical_Texts'" variety.js
command = 'mongo ustc --eval "var collection = \'Medical_Texts\'" variety.js > {}.txt'
# command = ['mongo', '-u', 'econ', '-p', 'crazyhorsemonument', 'ustc', '--eval', '"var collection = \'Medical_Texts\'"', 'variety.js']
with open('collections.csv') as fd:
    collections = fd.readline().split(',')
    for c in collections:
        c = command.format('../'+c)
        print(c)
        subprocess.run(c, shell=True, cwd='variety')
