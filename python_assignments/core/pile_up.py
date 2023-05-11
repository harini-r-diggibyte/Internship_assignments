from collections import deque


def piling_up(deq, n):
    max_el = max(deq[0], deq[-1])
    for el in range(n):
        left = deq[0]
        right = deq[-1]
        if max_el >= left and left >= right:
            max_el = deq.popleft()
        elif max_el >= right and right >= left:
            max_el = deq.pop()
    if len(deq) == 0:
        print("Yes")
    else:
        print("No")


t = int(input())

for i in range(t):
    n, nl = int(input()), deque(map(int, input().split()))
    piling_up(nl, n)