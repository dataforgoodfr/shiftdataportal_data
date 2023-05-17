# SDP Get Interface
# Define abstract methods for creating new source Api or Csv

import os
import requests
import re
from bs4 import BeautifulSoup as bs
from urllib.parse import urlencode
import pandas as pd
import json 
import csv
import traceback

data_raw = 'data/_raw/'
data_processed = 'data/_processed/'

# ---------------------------------------------------------------------------------------------
# # SDP raw source (Base class)
# ---------------------------------------------------------------------------------------------
class Raw(): 
    
    # Init (may not be the state of art but found more readable)       
    base_url = None
    base_params = None
    
    routes = []
        
    def __init__(self, base_url=None, base_params=None, route=None):
        # Base Url
        ## for api: https://api.eia.gov/v2
        ## for file(s): https://www.iea.org/data-and-statistics/data-sets 
        ## ...      
        self.base_url = base_url or self.__class__.base_url
        # Base url params
        ## {'api_key': ...}
        self.base_params = base_params or self.__class__.base_params or {}
        self.route = route
        
    # ---------------------------------------------------------------------------------------------
    # Public
      
    @classmethod    
    def main(cls, raw, test):
        if (test):
            cls._routes()
        elif raw:
            cls.to_raw()    
        else:
            cls.to_processed()  
            
    @classmethod   
    def to_raw(cls):
        # Loop over class level api routes
        base_url = cls.base_url 
        print(f"-- BASE URL: {base_url}")
        for route in cls._routes():
            inst =  cls(base_url, cls.base_params, route)                    
            print(f"\n-- Processing route: {route}")
            inst._raw_to_csv()  
    
    def to_processed(cls):
        pass
              
    # ---------------------------------------------------------------------------------------------
    # Private
    @classmethod
    def _routes(cls):
        print('\nFound routes: ' + str(cls.routes))
        return cls.routes
        
    # Url based on instance properties
    def _get_url(self):
        return self.base_url + ('' if not self.base_params else '?' + urlencode(self.base_params))
    
    def _response_data(self, response, route):
        pass

    # Instance Class        
    def _cls(self):
        return type(self)   

    # Instance Class name to lower case        
    def _cls_name(self, csv = False):
        cls_name = self._cls().__name__.lower() 
        if (not csv):
            return cls_name.split('_')[0]
        return cls_name  

    # CSV related methods
    def _csv_path(self, raw):
        csv_path = (data_raw if raw else data_processed) + self._cls_name() + '/'
        # Ensure creation of path (need it for dataframe to_csv)
        if not os.path.exists(csv_path):
            os.makedirs(csv_path)
        return csv_path
    
    def _csv_name(self):
        # format csv name
        replace = re.compile('-|/|\s+')
        route = ''
        info=False
        if (self.route is not None) :
            if ('csv_name' in self.route):
                route = self.route['csv_name']
            else:
                route = self.route['route']            
            info = bool(self.route.get('info'))
                
        return ('__info_' if info else '') + (self._cls_name(True) + replace.sub('_', route or '')).lower() 
       
    def _csv_full_name(self, raw=True):
        return self._csv_path(raw) + self._csv_name() + '.csv'
    
    # Default raw to csv via dataframe (_raw_df)
    def _raw_to_csv(self):
        # Raw Csv Output file
        csv_filename = self._csv_full_name(True)
        try:
            # All base route subroutes in same csv
            if 'routes' in self.route:
                df = pd.DataFrame()
                for route in self.route['routes']:
                    cls = self._cls()
                    inst =  cls(cls.base_url, cls.base_params, route)
                    route_df = inst._raw_df()  
                    if route_df is not None:
                        df=pd.concat([df, route_df], ignore_index=True)                     
            else:    
                # Get dataframe
                df = self._raw_df() 
            
            if df is not None and not df.empty: 
                # Ok, backup to raw csv    
                df.to_csv(csv_filename, index=False, encoding='utf-8') 
                if not os.path.exists(csv_filename):
                    raise FileNotFoundError(f"[FATAL] : Csv file not found!")          
            
        except Exception as e:
            print('[ERROR] Raw to csv: ' + csv_filename)
            print('[ERROR] Exception:\n' + str(e))
            traceback.print_exc()
            
    def process_raw_df(self):
        pass            

    # TODO or not
    @property
    def df(self, raw):
        if self._df is None:
            self._df = self.get_df()
        return self._df


              
# ---------------------------------------------------------------------------------------------
# # SDP Raw API 
# ---------------------------------------------------------------------------------------------
class Api(Raw):

    # Api routes dicts tuple
    max_results = 10000
       
    # ---------------------------------------------------------------------------------------------
    # Public
 
    # Raw url to raw csv 
    @classmethod   
    def to_raw(cls):
        # Loop over class level api routes
        base_url = cls.base_url 
        print(f"-- API BASE URL: {base_url}")
        for route in cls._routes():
            inst =  cls(base_url, cls.base_params, route)                     
            print(f"\n-- Processing API route: {route}")
            inst._raw_to_csv()    
                 
    
    # ---------------------------------------------------------------------------------------------
    # Private
    
    def _response_data(self, response, route = None):
        data = response.json()
        return data, route        
    
    # Url based on instance properties
    def _get_url(self):
        route_path = ''
        route_params = {}
        if self.route:
            route_path = self._route_path()
            route_params = self.route.get('route_params') or {}
        
        url =  self.base_url + route_path
        params =   {**(self.base_params or {}), **(route_params)}
        return url + ('' if not params else '?' + urlencode(params))          
       
    def _route_path(self):
        if self.route:
            return self.route['route']
        return ''

    # Default raw to dataframe
    def _raw_df(self):
        data = None       
        with requests.Session() as session:
            response = session.get(self._get_url())
            data = self._response_data(response)[0]
           
        if not data: 
            return None
            
        return pd.json_normalize(data)
    
    # Default page data
    def _page_data(self, data):        
        return data    
    
    # Default page params
    def _page_params(self, page):
        pages_params = self.route.get('route_params') or {}
        pages_params['page'] = page + 1
        return pages_params
        
    # Default route page 
    def _page_df(self, session, page):
        
        max = self.max_results
        
        offset = page * max        
        print('... Results ' + str(offset + 1) + ' - ' + str(offset + max))
        
        page_params = self._page_params(page)
            
        self.route['route_params'] = {**(self.route.get('route_params') or {}), **(page_params)}
        
        # Send the GET request
        response = session.get(self._get_url())
        #print(response.url)                
        #data = json.loads(response.text)
        data = self._response_data(response)[0]
        
        if data is not None:                                              
            try :
                page_df = pd.json_normalize(self._page_data(data))
                return page_df
            except Exception as e:
                print('Exception during json normalize for df!!\n' + e)
                #print(json.dumps(data, indent=4))
                
        return None        

    
    # Default raw to dataframe
    def _pages_df(self):
        
        route = self.route 
        if not route:
            return None

        pages_df = pd.DataFrame()
              
        # Test first level fot the route
        with requests.Session() as session:
            
            # Define pages for loop
            max = self.max_results
            total = int(route['total'])
            pages = total // max
            if total % max > 0:
                pages += 1
            
            # Pages loop
            for page in range(0, pages):
                page_df = self._page_df(session, page)
                if (not page_df.empty):
                    pages_df = pd.concat([pages_df, page_df], ignore_index=True)  
        
        return pages_df   
      
                           
    # Default raw to csv via dataframe (_raw_df)
    def _raw_to_csv(self):
        # Raw Csv Output file
        csv_filename = self._csv_full_name(True)
        try:
            # All base route subroutes in same csv
            if 'routes' in self.route:
                df = pd.DataFrame()
                for route in self.route['routes']:
                    cls = self._cls()
                    inst =  cls(cls.base_url, cls.base_params, route)
                    route_df = inst._raw_df()  
                    if route_df is not None:
                        df=pd.concat([df, route_df], ignore_index=True)                     
            else:    
                # Get dataframe
                df = self._raw_df() 
            
            if df is not None and not df.empty: 
                # Ok, backup to raw csv    
                df.to_csv(csv_filename, index=False, encoding='utf-8') 
                if not os.path.exists(csv_filename):
                    raise FileNotFoundError(f"[FATAL] : Csv file not found!")          
            
        except Exception as e:
            print('[ERROR] Raw to csv: ' + csv_filename)
            print('[ERROR] Exception:\n' + str(e))
            traceback.print_exc()

            
    # Other option using stream, no dataframe
    def _stream_to_csv(self, raw):
        with requests.Session() as session:
            response = session.get(self._get_url(), stream=True)
            # Open a new CSV file for writing
            with open(self._csv_full_name(raw), mode='w', newline='') as csv_file:
                # Initialize a CSV writer object
                writer = None

                # Parse JSON response incrementally and write data rows
                for row in response.iter_lines():
                    # Skip blank lines
                    if row:
                        # Convert JSON to Python object
                        data = json.loads(row)

                        # Create header row on first iteration
                        if not writer:
                            fieldnames = list(data[0].keys())
                            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                            writer.writeheader()

                        # Write data rows to CSV file
                        for d in data:
                            writer.writerow(d)


# ---------------------------------------------------------------------------------------------
# # SDP Raw FILE(S)
# ---------------------------------------------------------------------------------------------           
# In this first version we will focus on CSV
# We might need to scrap for file links (iea TODO, ember TODO)
# TODO OR NOT for other file types
class File(Raw):    
        
    def _pattern(self):
        search = self.route['search']
        search = search.replace('.','@@').replace('*','.*').replace('@@','\.')
        return r'^.*' + search + '.*$'     
        
    # Get csv list according to pattern
    def find_files(self, pattern = r'.*\.csv$'):
        with requests.session() as session:
            # Get response
            response = session.get(self.route['search_url'])
            pattern = r'href=["\'](.*?)["\']'
            links = re.findall(pattern, response.text)
            # Filter the links based on the pattern
            filtered_links = [link for link in links if re.match(self._pattern(), link)]
            return filtered_links

    # get The csv url
    def find_file(self):
        the_csv = []
        csvs = self.find_files(self._pattern())
        for csv in csvs:
            if csv in the_csv:
                continue
            the_csv.append(csv)

        if len(the_csv) > 0:
            return the_csv[0]

        return None

    def _get_url(self):
        file_url = self.find_file()
        return self.base_url + file_url
         
    def _raw_df(self):
        return pd.read_csv(self._get_url(), encoding='utf-8')
        
        
    """ TRACKING TODO
    def _raw_df(self, url, pattern):
        csv_url = self._get_url(url, pattern)
        df = None

        if (csv_url is not None):

            # Extract the CSV file name from the URL and remove the file extension
            csv_file_name = os.path.splitext(csv_url.split('/')[-1])[0]

            # Check if the tracking file already exists in the csv sub folder
            track_path = cur_dir + f"/files/.track/.{csv_file_name}"
            last_csv = cur_dir + "/files/cada_pradas_last.csv"

            if os.path.exists(track_path) and os.path.exists(last_csv):
                print(f"The tracking file '{track_path}' already exists.")

            else:
                with requests.Session() as session:
                    print(f"Getting csv from url : {csv_url}")
                    csv_content = session.get(csv_url).content.decode('utf-8')

                # Save the content of the response to a file
                with open(last_csv, mode='w', encoding='utf-8') as file:
                    file.write(csv_content)

                # Create a new blank file with the initial CSV file name to track csv versions
                with open(track_path, 'w'):
                    pass

                print(f"The file '{csv_file_name}.csv' has been uploaded to '{last_csv}'.")
                print(f"The tracking file '{track_path}' has been created.")

        if os.path.exists(last_csv):
            df = pd.read_csv(last_csv)


        # print number of rows
        print('CSV dataframe nb rows: '+str(len(df)))
        return df

    """



