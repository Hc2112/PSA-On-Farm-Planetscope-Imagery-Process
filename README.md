# PSA-On-Farm-Planetscope-Imagery-Process
Documented process for ordering, downloading, and processing Planet data extractions across the PSA network. <br><br>

<b>psa_planet_orders_summary.csv</b><br>
A summary of Planetscope surface reflectance imagery (4 or 8 band) that's been acquired for the On-Farm network. Metadata description below.<br><br>

<i>code</i>: Character. Field boundary code<br>
<i>count</i>: Integer. Count of unique imagery dates acquired for the code <br>
<i>date.min</i>: Date. The earliest imagery date acquired for the code<br>
<i>date.max</i>: Date. The latest imagery date acquired for the code<br>
<i>product.count</i>: Integer. 1= only 4-band imagery available for the code. 2= both 4-band and 8-band imagery are available.<br>
<i>date.range</i>: Integer. The date range for the acquired imagery for the code<br>
<i>cover_planting</i>: Date. The planting date for the code. Planting dates were found to be the same between reps within respective field codes. If an NA value existed,
the cover_planting date was defaulted to September 1 for that cover crop year. The planting date for field code LHR was changed from 2021-09-14 to 2022-09-14.<br>
<i>cc_termination_date</i>: Date. The cover crop termination date. Termination dates were found to be the same between reps within respective field codes. If an NA value existed, the cover_planting date was defaulted to June 1 for that cover crop year. The termination date for field code VSJ was changed from 2020-05-02 to 2021-05-02.<br>
<i>date.range.fields</i>: Integer. The number of days between cover_planting and cc_termination_date<br>
<i>treatment</i>: Character. C = ???????<br>
<i>type</i>: Character. Corner??????<br>
<i>cc_harvest_date</i>: Date. The cover crop harvest date.<br>
<i>cc_species</i>: Character. The cover crop species.<br>
<i>date.range.diff</i>: Integer. The difference in days between date.range and date.range.fields.<br>
<i>planting.min.diff</i>: Integer. The difference in days between cover_planting and date.min.<br>
<i>term.max.diff</i>: Integer. The difference in days between cc_termination_date and date.max. <br>
<i>Flagged</i>: Binary. 0= No apparent issues between imagery acquisition and agronomic data exist. 1= row needs review. There was either a difference greater than 7 days for either the planting.min.diff or term.max.diff column.



<b>RESOURCES</b><br>
Imagery: Planetscope imagery can be accessed from https://g-a69314.7ce1a.03c0.data.globus.org/Imagery/PSA%20Planet/ALL%20IMAGERY.zip<br><br>
Field boundaries: boundaries used for Planet API orders can be accessed from https://g-a69314.7ce1a.03c0.data.globus.org/Imagery/PSA%20Planet/Planet%20Extractions/GIS%20Files%20for%20Orders%20-%20CONFIDENTIAL.zip. These boundaries are different from the original boundaries from Biomass_polygon1.geojson in that they were split into their respective farm codes, buffered out by 30m, and reprojected to WGS84, EPSG 4326.
