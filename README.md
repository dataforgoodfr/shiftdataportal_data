# SDP Data Module

## Minimal documentation (TODO)

This project aims at retrieving "raw" datasets for the Shift Data Portal (The Shift Project).

"Raw data" sources/organisations:
- **WB**: World Bank Group
- **IEA**: International Energy Agency
- **EIA**: U.S. Energy Information Administration
- **BP**: BP Group
- **EMBER**: Ember-Climate.Org
- **OWID**: Our World In Data (TODO)

## Code:
### /src/sdp_data/main.py
Main module<br>
Just RUN IT AS IS to loop over all "Raw" sources (BP, EIA, EMBER, IEA, WB), save them to csv files and pack/zip them into /data/\_raw.7z

### /src/sdp_data/raw.py
Define default behaviour of the two main base classes Api(Raw) and File(Raw)

### /src/sdp_data/sources/
Folder listing the "Raw Data" sources modules.<br>
Each raw_{source}.py define configuration/implementation of the Api and/or File class(es) for a specific "Raw Data" {source} (ie: raw_wb.py for World Bank "Raw Data" source)

## Contributing

The next steps will use the Linux command lines. The documentation for Windows system will be added later, meanwhile, 
you can have a look at this [documentation](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/)
to create your virtual environment and install the packages.

First clone the git repository to your local machine.

`git clone https://github.com/dataforgoodfr/shiftdataportal_data.git`

Then you need to create a virtual environment in the repository you've just cloned and install the required packages.

```
cd shiftdataportal_data
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

After this, you should be ready to contribute!