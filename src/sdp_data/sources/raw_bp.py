# BP
import raw

class Bp_File(raw.File):
    
    base_url = 'https://www.bp.com'      
    routes = [
        #https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/xlsx/energy-economics/statistical-review/bp-stats-review-2022-consolidated-dataset-narrow-format.csv
        {'route': '/stats_review',
            'search_url': 'https://www.bp.com/en/global/corporate/energy-economics/statistical-review-of-world-energy/downloads.html',     
            'search': 'bp-stats-review*dataset-narrow-format.csv'
        }
    ] 
           

# --------------------------------------------------------------------
def main(raw, test):
    Bp_File().main(raw, test)
