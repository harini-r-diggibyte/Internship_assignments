if __name__ == '__main__':
    students = []
    grade_list = []
    N = int(input())
    for i in range(N):
        name = input()
        grade = float(input())
        students.append([name, grade])
        grade_list.append(grade)

    minscore = min(grade_list)
    indexlist = grade_list.index(minscore)

    for j in range(len(grade_list)):
        if minscore==students[j][1]:
            grade_list.pop(indexlist)
            gradelist_in_order = sorted(grade_list)

    sec_low_list = []

    for j in range(len(students)):
        if gradelist_in_order[0] == students[j][1]:
            sec_low_list.append(students[j][0])

    namelist_sec_low = sorted(sec_low_list)

    for i in range(len(namelist_sec_low)):
        print(namelist_sec_low[i])
