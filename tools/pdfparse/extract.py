import sys
import csv
#from PyPDF2 import PdfFileWriter, PdfFileReader
import tabula
import os

def extract(input, output, pages, area):
    print("Reading {} page {} with area {}".format(input, pages, area))
    df = tabula.read_pdf(input, pages=pages, stream=True, guess=False, encoding='utf-8', area=area)[0]
    df.to_csv(output, encoding='utf-8', index=False)

if __name__ == '__main__':
    if(len(sys.argv) <= 1):
        print("Usage: python extract.py pdf_path pages top left bottom right")

    path = sys.argv[1]
    pages = sys.argv[2]
    area = (int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))
    output = '{}.csv'.format('.'.join(path.split('.')[:-1]))

    extract(path, 'temp.csv', pages, area)

    # The table is weirdly formatted so some manual tweaking is required
    header = ["ICU Level", 
            "Region", 
            "LHIN",
            "# Critical Care Beds", 
            "# Critical Care Patients", 
            "# Vented Beds", 
            "# Vented Patients", 
            "# Suspected COVID-19",
            "# Suspected COVID-19 Patients with Invasive Ventilation", 
            "# Confirmed Positive COVID-19", 
            "# Confirmed Positive COVID-19 Patients with Invasive Ventilation", 
            "# Confirmed Negative COVID-19"]

    prepend = ['Level 3, West',
            'Level 3, West',
            'Level 3, West',
            'Level 3, West',
            'Level 3, Central',
            'Level 3, Central',
            'Level 3, Central',
            'Level 3, Central',
            'Level 3, Toronto',
            'Level 3, East',
            'Level 3, East',
            'Level 3, East',
            'Level 3, North',
            'Level 3, North',
            'Level 3, West',
            'Level 3, West',
            'Level 3, West',
            'Level 3, West',
            'Level 2, Central',
            'Level 2, Central',
            'Level 2, Central',
            'Level 2, Central',
            'Level 2, Toronto',
            'Level 2, East',
            'Level 2, East',
            'Level 2, East',
            'Level 2, North',
            'Level 2, North',
            ', Grand Total']

    # Open up csv and add our header/prepent
    with open('temp.csv', "r") as infile:
        with open(output, "w") as outfile:
            readCSV = csv.reader(infile, delimiter=',')
            writeCSV = csv.writer(outfile, delimiter=',')
            writeCSV.writerow(header)
            i = 0
            for row in readCSV:
                combinedRow = prepend[i].split(',') + row

                # Edge case row rules
                if i == len(prepend)-1:
                    combinedRow[2] = ''


                writeCSV.writerow(combinedRow)
                i = i + 1

    os.remove('temp.csv')
    print("Done, see {}".format(output))

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

