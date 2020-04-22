import sys
import csv
#from PyPDF2 import PdfFileWriter, PdfFileReader
import tabula
import os
import math

def extract(input, output, pages, area):
    print("Reading {} page {} with area {}".format(input, pages, area))
    df = tabula.read_pdf(input, pages=pages, stream=True, guess=False, encoding='utf-8', area=area)[0]
    df.to_csv(output, encoding='utf-8', index=False)

def extractCCSO(argv):
    path = argv[1]
    pages = argv[2]
    area = (int(argv[3]), int(argv[4]), int(argv[5]), int(argv[6]))
    date = ' '.join('.'.join(path.split('.')[:-1]).split(' ')[1:])
    output = '{}.csv'.format('.'.join(path.split('.')[:-1]))

    extract(path, 'temp.csv', pages, area)

    # The table is weirdly formatted so some manual tweaking is required
    header = ["Region",
            "LHIN",
            "# Critical Care Beds",
            "# Critical Care Patients",
            "# Baseline Vented Beds",
            "# Expanded Vented Beds",
            "# Vented Patients",
            "% Ventilator Capacity Remaining",
            "# Suspected COVID-19",
            "# Confirmed Positive COVID-19",
            "# Confirmed Positive COVID-19 Patients with Invasive Ventilation",
            "# Patients in Expanded ICU",
            "# COVID Positive Patients in Expanded ICU"]

    prepend = ['West,L1: ESC','West,L2: SW','West,L3: WW','West,L4: HNHB','Central,L5: CW','Central,L6: MH','Central,L8: Central',
    'Central,L12: NSM','Toronto,L7: Toronto','East,L9: CE', 'East,L10: SE', 'East,L11: Champlain',
    'North,L13: NE', 'North,L14: NW']


    def parsenum(value):
        if value == '':
            return 0
        try:
            val = float(value)
            return int(val)
        except ValueError:
            try:
                val = int(value)
                return val
            except ValueError:
                return value

    # Open up csv and add our header/prepent
    with open('temp.csv', "r") as infile:
        with open(output, "w") as outfile:
            readCSV = csv.reader(infile, delimiter=',')
            writeCSV = csv.writer(outfile, delimiter=',')
            writeCSV.writerow(header)
            i = 0
            for row in readCSV:
                combinedRow = prepend[i].split(',') + row
                combinedRow = [parsenum(x) for x in combinedRow]
                writeCSV.writerow(combinedRow)
                i = i + 1

    os.remove('temp.csv')
    #print("Done, see {}".format(output))

if __name__ == '__main__':
    if(len(sys.argv) <= 1):
        print("Usage: python extract.py pdf_path pages top left bottom right")

    extractCCSO(sys.argv)

# If we need to crop for whatever reason
'''def crop(path, num=0, ll=(0,0), ur=(100, 100)):
    print("Cropping {} page {} at ll:{} ur:{}".format(path, num, ll, ur))
    print("Width {} height {}".format(ur[0] - ll[0], ur[1] - ll[1]))
    pdf_file = PdfFileReader(open(path, "rb"))
    page = pdf_file.getPage(num)

    page.mediaBox.lowerLeft = ll
    page.mediaBox.lowerRight = (ur[0], ll[1])
    page.mediaBox.upperLeft = (ll[0], ur[1])
    page.mediaBox.upperRight = ur

    output = PdfFileWriter()
    output.addPage(page)
    with open("cropped.pdf", "wb") as out_f:
        output.write(out_f)'''
