def print_formatted(number):
    width = len(format(number, "b"))
    for i in range(1, number + 1, 1):
        dec = str(i).rjust(width,' ')
        oct = format(i, "o").rjust(width,' ')
        hex = format(i, "X").rjust(width,' ')
        bin = format(i, "b").rjust(width,' ')
        print(dec,oct,hex,bin)
   #rjust(width_of_string,(optional)padding space)
   #(.i.e)rjust (width)is same as written code.
if __name__ == '__main__':
    n = int(input())
    print_formatted(n)