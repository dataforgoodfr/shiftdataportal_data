# WORLDBANK
import raw
 
class Wb_Api(raw.Api):
    
    max_results = 21000 # API MAX=32000 but  
    base_url = 'https://api.worldbank.org/v2' 
    base_params = {
        'format': 'json',
        'per_page' : max_results
    } 
        
    routes = [
        # Info data
        {'route': '/regions', 'info': True},
        {'route': '/countries', 'info': True},
        {'route': '/topics', 'info': True},
        {'route': '/sources', 'info': True},
        {'route': '/indicators', 'info': True},
        
        # Actual data
        {'route': '/country/all/indicator', 'csv_name' : '/indicator',
            'groups': [
                # TODO see with Zeynep
                {'group' : 'pop',  'pattern' : 'SP.POP.TOTL*' },
                {'group' : 'gdp',  'pattern' : 'NY.GDP.MKTP.*'},
                
                # ENERGY/CLIMATE THEMES
                {'group' : 'ghge_sect',  'pattern' : '*GHG*EMSE*'},
                #{'CO2': '*CO2*'},
                #{'USE': '*USE*'},
                
                # KAYA
                # {'GDP_PCAP': 'NY.GDP.PCAP.*'},
                # {'GDP_PUSE': 'EG.GDP.PUSE.*'},
                # ... TODO search for all equation factors (some seem obsolete)                                
            ]
         }
    ]
    
    # Get info dataframe for a particular info route
    @classmethod
    def _info_df(cls, info):
        info_route = next(filter(lambda route: bool(route.get('info')) and route['route'] == '/' + info, cls.routes), None)
        info_inst = cls(cls.base_url, cls.base_params, info_route)
        info_df = pd.read_csv(info_inst._csv_full_name())
        #print(info_df.head())
        return info_df
    
    @classmethod
    def _indicators(cls, search):
        
        df_indicators = cls._info_df('indicators')
        
        # Explode sources
        df_indicators['source'] = df_indicators['source'].apply(lambda x : dict(eval(x)) )
        source_exploded = df_indicators['source'].apply(pd.Series).add_prefix('source.')
        df_indicators = pd.concat([df_indicators.drop('source', axis=1), source_exploded], axis=1)
        
        # Exclude archives = 57
        df_indicators = df_indicators[df_indicators['source.id']!='57']
        
        # Exclude: relative indicators (in %)
        if '.ZS' not in search:
            df_indicators = df_indicators[~df_indicators['id'].str.endswith('.ZS')]    
        if '.ZG' not in search:
            df_indicators = df_indicators[~df_indicators['id'].str.endswith('.ZG')] 
                    
        # Exclude : Found indicator ending with XU.E (ex: Gdp deflator)
        # df_indicators = df_indicators[~df_indicators['id'].str.endswith('.XU.E')]
        
        # Matches pattern
        if '*' in search:
            search = search.replace('.','@@').replace('*','.*').replace('@@','.')
        pattern = r'^' + search + '$'
        
        indicators = tuple(df_indicators.loc[df_indicators['id'].str.contains(pattern), 'id'])

        print(indicators)
        return indicators
    
    # Override method
    @classmethod
    def _routes(cls):
        
        _routes = []  
        # Test first level for the route and deeper if sub routes
        print('-- Analyzing routes, please wait...')
        with requests.Session():
                     
            for route in cls.routes:
                
                _route = dict(route)
                
                if (bool(_route.get('info'))):
                    continue                                          
                
                if 'groups' in _route:
                    
                    route_path = _route['route']                                 
                    route_params = _route.get('route_params') or {}
                    route_csv_name = _route['csv_name']  
                                       
                    groups =  _route['groups'] 
                    _route.pop('groups') 

                    # We iterate on all indicator groups     
                    for group in groups:
                                                 
                        group_route = dict(_route)
                        
                        group_name = group['group']                        
                        group_csv_name =  route_csv_name + '/' + group_name  
                        group_route['csv_name'] = group_csv_name  
                        
                        print('\n --- group route ' + str(group_route))                                         
                        
                        # Get all actual routes for this group                         
                        group_routes = cls._indicators(group['pattern'])                        
                        
                        _route_routes = []
                        for sub_route in group_routes:
                
                            # We build actual WB indicator route                           
                            sub_path = route_path +'/'+ sub_route
                            print('sub_path:' + sub_path)
                            # Lowest per_page (default is 50, 1 not allowed)
                            sub_params = {'per_page': 10}
                            
                            # Here we can access /data path directly (actual api sub route)
                            response = requests.get(cls.base_url + sub_path, {**(cls.base_params), **(route_params), **(sub_params)})
                            #print('!!! response.url' + response.url)
                            sub_data, sub_route =  cls()._response_data(response, group_route)

                            if not sub_route:
                                continue  
                            
                            # Sub route returns results                                                                     
                            _route_routes.append({**sub_route, **({'route': sub_path})})
                        
                        # Set routes group for the route
                        if (_route_routes):  
                            group_route['routes'] =  _route_routes 
                        #print(group_route)
                        _routes.append(group_route)

                else:                            
                    # Append base route                 
                    _routes.append(_route) 
                
        print('\nFound routes: '+ str(_routes))                    
        return _routes 
    
    
    # Override method
    def _response_data(self, response, route = None):       
        data = response.json()
        # Ensure we have only one page (all results)
        if 'per_page' in data[0] and (int(data[0]["per_page"]) == self.max_results):
            if int(data[0]["pages"]) > 1:
                raise Exception('ERROR: Several pages found. It is a bug!')
        else:
            if 'total' in data[0]:
                total = int(data[0]['total'])
                # Skip this route if total results == 0
                if not total :
                    return None, None
                else:
                    route['total'] = total   
            
            return data[0], route                  
            
        # Interesting data              
        return data[1], route 
        
    # Override method
    def _raw_df(self):
        return self._pages_df()    
       
#----------------------------------------------------------------
def main(raw, test):
    Wb_Api().main(raw, test)
    
###########################################################################################################

import io
import zipfile

import pandas as pd
import requests
from sqlalchemy import create_engine

# BACKUP CODE CHRISTOPHE
# EASY WAY TO POSTGRE
def christophe():
    url = "https://api.worldbank.org/v2/en/indicator/'SP.POP.TOTL?downloadformat=csv"
    output_csv = "API_'SP.POP.TOTL_DS2_en_csv_v2_5436324.csv"
    response = requests.get(url)
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    csv_file = zip_file.open(output_csv)
    df = pd.read_csv(csv_file, skiprows=4)
    df = df.filter(regex='^(?!Unnamed)')
    df = df.drop(columns=["Indicator Name", "Indicator Code"])
    df = df.melt(id_vars=["Country Code", "Country Name"], var_name="Year", value_name="Population")
    postgres_url = "postgresql://..."  # To fill with a valid postgresql
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        df.to_sql("worldbank_population", conn, if_exists="replace", index=False)
        