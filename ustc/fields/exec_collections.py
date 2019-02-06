import subprocess
#  mongo -u econ -p crazyhorsemonument ustc --eval "var collection = 'Medical_Texts'" variety.js
command = ['mongo ustc --eval "var collection = \'Medical_Texts\'" variety.js']
# command = ['mongo', '-u', 'econ', '-p', 'crazyhorsemonument', 'ustc', '--eval', '"var collection = \'Medical_Texts\'"', 'variety.js']
with open('collections.csv') as fd:
    collections = fd.readline().split(',')
    for c in collections:
        print(c)
        x = subprocess.run(command, shell=True, cwd='variety')
        print('x:', x)
        exit()
