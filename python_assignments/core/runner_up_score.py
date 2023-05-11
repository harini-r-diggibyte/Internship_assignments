if __name__ == '__main__':
    n = int(input())
    arr = map(int, input().split())
    l = list(arr)
    a = max(l);
    while a in l:
        l.remove(a)
    print(max(l))

    #for i in l:
     #   if l[i] == a:
      #      l.remove(i)
    #print(l)
