import math
import statistics
from functools import reduce
#map
def area(r):
    return math.pi * (r**2)
raddi = [2,4,6,4,2.3,5.6]
print(list(map(area,raddi)))

temp = [('berlin',29),('hanoi',35),('dalat',22),('new york',30),('london',20),('moicovc',25)]

c_to_f = lambda data:(data[0],(4.5)*data[1]+32)
print(list(map(c_to_f,temp)))

#filter
data = [1.3,2.4,5.6,3.2,4.5,5.4]

avg= statistics.mean(data)
print(list(filter(lambda x:x>avg,data)))


countries  =  ['nhatrang','','dalat','&','saigon','','hanoi','']

print(list(filter(None,countries)))

#reduce
multifier  = lambda x,y:x*y
print(reduce(multifier,data))