if __name__ == '__main__':
    n = int(input())
    student_marks = {}
    for _ in range(n):
        name, *score = input().split()
        scores_list = list(map(float, score))
        student_marks[name] = scores_list
    query_name = input()
    allscores = student_marks[query_name]
    avg = sum(allscores) / len(allscores)
    print(format(avg, ".2f"))
    