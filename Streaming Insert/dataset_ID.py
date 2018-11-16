import time
for i in range(1000000):
    if i != 0 and i % 500==0:
        time.sleep(10)
        print (i)