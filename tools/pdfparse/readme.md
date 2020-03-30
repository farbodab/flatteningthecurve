# PDF Parser
Basic python tool to extract a table from a pdf.

Right now the tool is catered to Critical Care Services Ontario (CCSO) COVID-19 Daily Report and assumes table headers and row count does not change.

It could be applied to other pdfs in the future if required with small tweaks.

## Requirements

https://github.com/chezou/tabula-py

```bash
pip install tabula-py
```

## Usage

```bash
python ./extract.py pdf_path pagenum top left bottom right

e.g.
python ./extract.py ./my_pdf.pdf 1 100 100 200 200
```

See CCSO.sh for execution compatible with given pdf.

## Measurements

Measurements are pdf "points"

In your adobe reader preferences you can change the units to "points" and use the measure tool to obtain these values

top/bottom are measured from top of pdf

left/right are measured from left of pdf

