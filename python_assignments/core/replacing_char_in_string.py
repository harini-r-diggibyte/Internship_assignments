def mutate_string(s, p, c):
    l1=list(s)
    l1[p]=c
    str1 = "".join(l1)
    return str1

if __name__ == '__main__':
    s = input()
    i, c = input().split()
    s_new = mutate_string(s, int(i), c)
    print(s_new)