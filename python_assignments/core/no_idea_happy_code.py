n,m = input().split()
arr=list(map(int,input().split()))
setA = set(map(int,input().split()))
setB = set(map(int,input().split()))
happy = 0
for i in arr:
    if i in setA:
        happy += 1
    elif i in setB:
        happy -= 1
print(happy)