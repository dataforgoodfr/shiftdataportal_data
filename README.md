# SDP Data Module

## Minimal documentation (TODO)

This project aims at retrieving "raw" datasets for the Shift Data Portal (The Shift Project).

"Raw data" sources/organisations:
<ul>
  <li>WB: World Bank Group</li>
  <li>IEA: International Energy Agency</li>
  <li>EIA: U.S. Energy Information Administration</li>
  <li>BP: BP Group</li>
  <li>EMBER: Ember-Climate.Org</li>
  <li>OWID: Our World In Data (TODO)</li>
</ul>

## Code:
### /src/sdp_data/main.py
Main module to download Raw Data and save it to csv<br>
Just RUN IT AS IS to loop over all "Raw" sources (BP, EIA, EMBER, IEA, WB), save them to csv files and pack/zip them into /data/\_raw.7z

### /src/sdp_data/raw.py
Define default default implementations for the two main base classes Api(Raw) and File(Raw)

### /src/sdp_data/sources/
Folder containing "Raw Data" sources modules.
Each raw_{source}.py define the class(es) for a specific "Raw Data" source
