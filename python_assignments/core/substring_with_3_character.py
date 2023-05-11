def merge_the_tools(string, k):
    y=""
    startIndex = 0
    while startIndex < len(string):
        endIndex = startIndex + k;
        for i in range(startIndex,endIndex):
            if (len(y) > 0):
                if string[i] not in y:
                    y+=string[i];
                    startIndex += 1
                else:
                    startIndex += 1
            else:
                y+=string[i];
                startIndex += 1
        print(y);
        y=""
if __name__ == '__main__':
    string, k = input().split()
    merge_the_tools(string, int(k))