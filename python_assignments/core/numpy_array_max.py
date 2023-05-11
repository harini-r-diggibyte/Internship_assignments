import numpy
n,m = input().split()
l=[]
for _ in range(int(n)):
    arr = numpy.array([list(map(int,input().split()))])
    mi = numpy.min(arr, axis = 1)
    l.append(mi)
l_array=numpy.array(l)
print(*max(l_array))