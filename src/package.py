#!/usr/bin/python3

import sys, os, re, os.path, datetime, glob

USAGE = "package.py <input_file_prefix> <date_iso_format>"

class Args:

    def __init__(self):
        self.exe = sys.argv[0]
        self.args = list(sys.argv)[1:]

    def next(self):
        if len(self.args) == 0:
            return None
        n = self.args[0]
        self.args = self.args[1:]
        return n

def main():
    args = Args()
    prefix = args.next()
    date = args.next()
    if not prefix:
        print(USAGE)
        sys.exit(1)
    if not date:
        date = datetime.date.today()
    else:
        date = datetime.date.fromisoformat(date)
    if args.next():
        print(USAGE)
        sys.exit(1)
    input_filenames = glob.glob("output/%s_%s_*.csv" % (prefix, date))
    output_filename = "output/Package_%s_%s.csv" % (prefix, date)
    with open(output_filename, "w") as f_out:
        with open(input_filenames[0], "r") as f_in:
            for line in f_in.readlines():
                print(line.rstrip(), file=f_out)
        for input_filename in input_filenames:
            with open(input_filenames[0], "r") as f_in:
                is_first = True
                for line in f_in.readlines():
                    if is_first:
                        is_first = False
                    else:
                        print(line.rstrip(), file=f_out)

if __name__ == "__main__":
    main()

