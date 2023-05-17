
# MAIN FILE FOR SDP
import os
import importlib
     
def main(source_list=None, raw=True, test=False):
    
    # Get the parent path of the current script
    cur_path = os.path.dirname(os.path.abspath(__file__))
    sources = 'sources'
    
    # loop through all raw sources in the folder
    for module in os.listdir(os.path.join(cur_path, sources)):
        # check if the file is a python module
        if module.startswith('raw_') and module.endswith('.py'):
            # extract the module name
            raw_name = module[:-3]
            
            if any(f'raw_{source}' in raw_name for source in list(source_list)):
                
                print()
                print('------------------------------------------------------------------------------------')
                print('-- RAW SOURCE MODULE NAME: ' + raw_name)
        
                # import the source module
                raw_module = importlib.import_module(sources + '.' + raw_name )

                # Run main method for this source
                raw_module.main(raw, test)
        
# Default behavior when running this file       
if __name__== "__main__":
    main() 

