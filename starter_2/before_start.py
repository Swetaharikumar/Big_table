import os
import consts as const
import glob

fileList = glob.glob('data*.txt')
for file in fileList:
    os.remove(file)
fileList = glob.glob('manifest*.txt')
for file in fileList:
    os.remove(file)
fileList = glob.glob('WAL*.txt')
for file in fileList:
    os.remove(file)

fileList = glob.glob('../data*.txt')
for file in fileList:
    os.remove(file)
fileList = glob.glob('../manifest*.txt')
for file in fileList:
    os.remove(file)
fileList = glob.glob('../WAL*.txt')
for file in fileList:
    os.remove(file)
print("remove successfully")