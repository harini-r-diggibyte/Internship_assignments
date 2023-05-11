import math
import os
import random
import re
import sys
from datetime import datetime

if __name__ == '__main__':
    t = int(input())
    for t_itr in range(t):
        t1 = input()
        t2 = input()
        str1=datetime.strptime(t1,"%a %d %b %Y %H:%M:%S %z")
        str2=datetime.strptime(t2,"%a %d %b %Y %H:%M:%S %z")
        tot_sec=int((str1-str2).total_seconds())
        print(abs(tot_sec))