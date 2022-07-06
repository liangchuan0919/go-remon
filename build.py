#Python 2.7.5
import os
import sys
srv = sys.argv[1]
cmd = 'go build -o build/bin/ go-remon.lc/src/' + srv
print(cmd)
os.system(cmd)

