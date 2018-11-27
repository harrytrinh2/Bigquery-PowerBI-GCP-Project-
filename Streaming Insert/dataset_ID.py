import time
for i in range(1000000):
    if i != 0 and i % 1500==0:
        print i
        print round((float(int(i) / float(1000000))*100),2) , "%"
