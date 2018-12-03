from time import localtime, strftime

def log(log_name, msg):
    with open('logs/{}'.format(log_name), 'a') as f:
        f.write('{} {}\n'.format(strftime('%Y-%d-%m_%H:%M:%S', localtime()), msg))
