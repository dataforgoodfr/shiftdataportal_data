# SDP Data Module

## Minimal documentation (TODO)

This project aims at retrieving "raw" datasets for the Shift Data Portal (The Shift Project).

"Raw data" sources/organisations:
<ul>
  <li><b>WB</b>: World Bank Group</li>
  <li><b>IEA</b>: International Energy Agency</li>
  <li><b>EIA</b>: U.S. Energy Information Administration</li>
  <li><b>BP</b>: BP Group</li>
  <li><b>EMBER</b>: Ember-Climate.Org</li>
  <li><b>OWID</b>: Our World In Data (TODO)</li>
</ul>

## Code:
### /src/sdp_data/main.py
Main module<br>
Just RUN IT AS IS to loop over all "Raw" sources (BP, EIA, EMBER, IEA, WB), save them to csv files and pack/zip them into /data/\_raw.7z

### /src/sdp_data/raw.py
Define default behaviour of the two main base classes Api(Raw) and File(Raw)

### /src/sdp_data/sources/
Folder listing the "Raw Data" sources modules.<br>
Each raw_{source}.py define the class(es) for a specific "Raw Data" {source} (raw_wb.py for World Bank "Raw Data" source)
  
