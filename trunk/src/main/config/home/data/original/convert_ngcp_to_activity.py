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
                    row[0],         # Project ID -> ID
                    row[2],         # Submit Year -> Submit Year
                    row[3],         # First year of funding -> Start Year
                    row[7],         # Project Title -> Title
                    row[8],         # Project Abstract -> Description
                    row[4],         # Administering Organisation -> Institution
                    row[6],         # Investigators -> Investigators
                    row[5]          # Discipline / Group -> Discipline
                ])
        csvfile.close()
        print >> sys.stderr, "wrote %s rows" % line
    else:
        print "file not found!: %s" % filename
