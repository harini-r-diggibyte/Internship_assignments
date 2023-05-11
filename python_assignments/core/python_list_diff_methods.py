if __name__ == '__main__':
    N = int(input())
    arry = []
    for i in range(N):
        inpt = input().split()

        if inpt[0] == "insert":
            arry.insert(int(inpt[1]), int(inpt[2]))
        elif inpt[0] == "print":
            print(arry)
        elif inpt[0] == "remove":
            arry.remove(int(inpt[1]))
        elif inpt[0] == "append":
            arry.append(int(inpt[1]))
        elif inpt[0] == "sort":
            arry.sort()
        elif inpt[0] == "pop":
            arry.pop()
        elif inpt[0] == "reverse":
            arry.reverse()