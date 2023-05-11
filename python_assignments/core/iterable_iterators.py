from itertools import combinations
count=0
n,ltt,k= int(input()),input().split(), int(input())
comb = list(combinations(ltt,int(k)))
for i in comb:
    if 'a' in i:
        count+=1
print(count/len(comb))