# https://www.covid-19-mc.ca/ Scraper

A scraper for the data found at https://www.covid-19-mc.ca/interactive-model

## Python
### Requirements
- Pyppeteer
- BeautifulSoup

### Installation
`pip3 install -r requirements.txt`

### Usage
`python3 scrape.py -o OUTPUT_PATH -s DATA_SOURCE
`   

Where `-s` can be:

- base_sk (Base case - "South Korea" Scenario)
- base_on (Base case - "Expected Ontario" Scenario)
- base_italy (Base case - "Italy" Scenario)
- expanded_sk (Expanded - "South Korea" Scenario)
- expanded_on (Expanded - "Expected Ontario" Scenario)
- expanded_italy (Expanded - "Italy" Scenario)
- base_on_n (Base Case - "Ontario North" Scenario)
- base_on_e (Base Case - "Ontario East" Scenario)
- base_on_w (Base Case - "Ontario West" Scenario)
- base_on_c (Base Case - "Ontario Central" Scenario)
- base_toronto (Base Case - "Toronto" Scenario)
