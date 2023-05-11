import numpy

n = int(input())
arr=[]
for i in range(n):
    arr.append(list(map(float, input().split())))
array1 = numpy.array(arr,float)
result = numpy.linalg.det(array1)
print(round(result,2))