# Ontario Health Unit IDEA Model

A scraper for the data found at https://art-bd.shinyapps.io/Ontario\_Health\_Unit\_IDEA\_model/

## Python
### Requirements
- Pyppeteer
- BeautifulSoup

### Installation
`pip3 install -r requirements.txt`

### Usage
`python3 scrape.py -o OUTPUT_PATH -s DATA_SOURCE
`   

Where `-s` can either be `on` for Ontario projections, or `health_unit` for health unit projections