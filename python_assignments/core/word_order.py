words_count = input()
words_list = []
unique_words ={}

for i in range(int(words_count)):
    word = input()
    words_list.append(word)
    if word not in unique_words:
        unique_words[word]=1
    else:
        unique_words[word] += 1

distinct = len(unique_words.keys())
print(distinct)
print(*unique_words.values())