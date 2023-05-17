#IEA : 
import raw

# No LICENCE nor REFERENCE (might become obsolete)
class Iea_Api(raw.Api):
    
    base_url = 'https://api.iea.org'      
    routes = [{'route' : '/eei-explorer', 'csv_name': '/eei'}]    


# TODO: implement? SDP LICENSE FOR FREE RESOURCE HAS TO BE CC-BY-NC-SA (some CC-BY 4.0 but not for datasets, only reports)
# See https://www.iea.org/terms
class Iea_File(raw.File):
    
    base_url = 'TODO?'      
    routes = []    


#----------------------------------------------------------------
def main(raw, test):
    Iea_Api().main(raw, test)
    