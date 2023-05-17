# EIA API
# https://api.eia.gov/v2/international/data/?frequency=annual&data[0]=value&facets[activityId][]=34&facets[productId][]=4701&facets[countryRegionId][]=WORL&facets[unit][]=BDOLPPP&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000

import requests
import time

import raw

class Eia_Api(raw.Api):
    
    # API Limit : max results per request
    max_results = 5000
        
    base_url = 'https://api.eia.gov/v2'
    base_params = {        
        # TODO CHANGE api_key WITH D4G OR SDP account 
        # TODO API KEYS MANAGEMENT (indeed for EIA we see api_key clearly in url...)
        'api_key': 'TYUMyndQZkshGBZTAM0tUfslaM1pvIctp1bcK7iV',
        'data[0]': 'value',
        'frequency': 'annual',
        'offset': '0',
        'length': max_results
    }

    routes =[
        {'route': '/international', 'csv_name': '/intl/region', 'data': True,
         'route_params': {
             'facets[countryRegionTypeId][]': 'r', # regions only 
             # Sorting 
             # first by period DESC
             'sort[0][column]': 'period', # sort by period
             'sort[0][direction]': 'desc',
             'sort[1][column]': 'activityId', # sort by activity
             'sort[1][direction]': 'asc'             
         }
        },
        {'route': '/ieo', 'csv_name': '/ieo/world', 'first' : True, 
         'route_params': {
             'facets[regionId][]': '0-0', # WORLD 
             # Sorting   
             'sort[1][column]': 'period', # sort by period
             'sort[1][direction]': 'desc',        
         }
        }
    ]
    
    @classmethod
    def _routes(cls):
        _routes = []
        # Test first level for the route and deeper if sub routes
        
        print('-- Analyzing routes, please wait...')
        with requests.Session():
            
            for route in cls.routes:
                
                route_path = route['route']                                 
                # Send the GET request
                params = {}
                if ('data' in route and bool(route['data'])):
                    route_path += '/data'
                    params = {'length': 1}
                
                route_params = route.get('route_params') or {}
                    
                response = requests.get(cls.base_url + route_path, {**(cls.base_params), **(route_params), **(params)})
                
                data, route =  cls()._response_data(response, dict(route))
                
                if not route:
                    continue   
                
                #print(route)  
                      
                # ieo dataset case    
                if 'routes' in data:  
                    sub_routes = data['routes'] 
                    if (sub_routes):
                        # Sort sub_routes DESCENDING to ensure firsts datasets are the latest                            
                        # Important: sub route for current year might be empty so we need to check
                        sub_routes = sorted(sub_routes, key=lambda x: x['id'], reverse=True)                
                        for sub in sub_routes:
                            # API throttling (seems to reduce or done at different time of of day (morning after Api counter reset), no clue :()
                            time.sleep(5)
                            
                            sub_path = route_path +'/'+ str(sub['id']) 
                            sub_params = {'length': 1}
                            
                            # Here we can access /data path directly (actual api sub route)
                            response = requests.get(cls.base_url + sub_path + '/data', {**(cls.base_params), **(route_params), **(sub_params)})
                            #print('!!! response.url' + response.url)
                            sub_data, sub_route =  cls()._response_data(response, dict(route))

                            if not sub_route:
                                continue  
                            
                            # Sub route returns results                                                                     
                            _routes.append({**(sub_route), **({'route': sub_path})})
                            # Test if route params includes 'first' to exit when first non zero dataset is found
                            if (bool(sub_route.get('first'))):
                                break
                        
                        # Skip appending base route        
                        continue
                
                # Append base route                 
                _routes.append(route) 
                 
                # API throttling 
                time.sleep(5)
                
        print('Found routes: '+ str(_routes))                    
        return _routes 
    
    # Override method
    def _response_data(self, response, route = None):
        
        #print('- Getting response for url: ' +response.url)
        
        data = response.json()
        #print('route: '+ str(route)) 
        #print('data: '+ str(data))
                                                  
        if 'error' in data:
            print('[DEBUG]:\n' + str(data))
            error = data['error']  
            if type(error) is not dict:
                error = {'code': data['code']  , 'message': error}
            
            # TODO if error['code']=='OVER_RATE_LIMIT'   
            raise Exception('[FATAL] Error ' + error['code'] + ': ' + error['message'] )

        if 'response' in data:
            # Get interesting data
            data = data['response']
            #print('data: '+ str(data))
            
            if 'total' in data:
                total = int(data['total'])
                # Skip this route if total results == 0
                if not total :
                    return None, None
                elif route is not None:
                    route['total'] = total   
            
            return data, route    
        
        #print('Response data: None !!')
        return None, None
    
    
    # Override method
    # Eia specific: /data path added to route to actually get it       
    def _route_path(self):
        route_path = super()._route_path()
        if route_path:
            route_path += '/data'
        return route_path    
    
    # Override method
    def _raw_df(self):
        return self._pages_df()
    
    # Override method
    def _page_params(self, page):
        pages_params = self.route.get('route_params') or {}
        pages_params['offset'] =  page * self.max_results
        return pages_params
    
    # Override method
    def _page_data(self, data):        
        return data['data'] 
    
# --------------------------------------------------------------------------------------------------------                    
def main(raw, test):
    Eia_Api().main(raw, test)
    