def fun(s):
    try:
        username,url=s.split("@")
        wn,ex=url.split(".")

    except:
        return False
    if (username.replace("-","").replace("_","").isalnum()):
        if (wn.isalnum()):
            if (ex.isalpha()):
                if (len(ex)<=3):
                        return True
    else:
        return False
def filter_mail(emails):
    return list(filter(fun, emails))

if __name__ == '__main__':
    n = int(input())
    emails = []
    for _ in range(n):
        emails.append(input())

filtered_emails = filter_mail(emails)
filtered_emails.sort()
print(filtered_emails)