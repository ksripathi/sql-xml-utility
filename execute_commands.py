import subprocess
#from __init__ import *

__all__ = ['execute_command']


def execute_command(cmd):
    return_code = -1
    output = None
    try:
        output = subprocess.check_output(cmd, shell=True)
        return_code = 0
    except subprocess.CalledProcessError as cpe:
        raise cpe
    except OSError as ose:
        raise ose

    return (return_code, output)


if __name__ == '__main__':
    '''
    cmd = "git clone " +\
        "https://github.com/ksripathi/resume.git " \
        "/home/sripathi/"
   '''
    cmd = "ls -la"

    try:
        (ret_code, output) = execute_command(cmd)
    except Exception, e:
        raise e
