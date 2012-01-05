#!/usr/bin/python

import csv
import os.path
import sys

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "usage: python %s input.csv" % __file__
        sys.exit(0)

    filename = sys.argv[1]
    if os.path.exists(filename):
        csvfile = open(sys.argv[1], 'rU')
        reader = csv.reader(csvfile)
        writer = csv.writer(sys.stdout)
        line = 0
        for row in reader:
            line = line + 1
            # skip header
            if line == 1:
                writer.writerow([
                    "ID",
                    "Submit Year",
                    "Start Year",
                    "Title",
                    "Description",
                    "Institution",
                    "Investigators",
                    "Discipline"
                ])
                continue
            # map rows to activity format
            fullrow = " ".join(row)
            if fullrow.strip() != "" and row[0] != "":
                writer.writerow([
                    row[0],         # Grant Id -> ID
                    row[11],        # App Year -> Submit Year
                    row[12],        # Start Year -> Start Year
                    row[10],        # Simplified Title -> Title
                    row[9],         # Scientific Title -> Description
                    row[2],         # Grant Admin Institution -> Institution
                    row[1],         # CIA Full Name -> Investigators
                    row[30]         # Main Category FOR -> Discipline
                ])
        csvfile.close()
        print >> sys.stderr, "wrote %s rows" % line
    else:
        print "file not found!: %s" % filename
