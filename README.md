<img width="65" height="21" alt="image" src="https://github.com/user-attachments/assets/25060a39-cc9d-46d1-83ff-757e5794c19b" /># PSA-On-Farm-Planetscope-Imagery-Process
Documented process for ordering, downloading, and processing Planet data extractions across the PSA network. <br><br>

The script for calling the Planet API requires an API key, which can be found under the user's Planet account profile under "My Settings". Anyone can create a Planet account, but to have download access to Planetscope 8-band imagery one must apply to the Commercial Satellite Data Acquisition (CSDA) program. The quota for CSDA accounts is 5,000,000 km2 per user.

Planet requires that areas-of-interest (AOIs) be projected in geographic coordinates (EPSG: 4326) and contain no more than 1,500 vertices. AOIs can be in .shp or .geojson file format, but must not contain multi-polyons. Ensure that your AOI is dissolved into one polygon and constrained to your feature(s) of interest in order to maximize on your Planet quota. Another factor that may prevent an order from executing is an AOI's topology. If such an error occurs, calling content(order.status)[['last_message']] will reveal where in the AOI the first error occurred. This can be an iterative process, so inspecting an AOI's topology using GIS software before attempting to place an order is recommended.
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
<i>Flagged</i>: Binary. 0= No apparent issues between available imagery  and agronomic data exist. 1= row needs review. There was either a difference greater than 10 days for for  planting.min.diff or term.max.diff, or the quotient of date.range.fields divided by count is greater than five days.<br><br>


<b>planet_scenes_metadata.csv</b><br>
A summary of downloaded Planetscope imagery metadata. <br><br>
<i>code</i>: Character. Field boundary code<br>
<i>id</i>: Character. Scene identification.<br>
<i>instrument</i>: Character. PS2 = Dove Classic. PS2.SD = Dove-R SuperDove. PSB.SD = 3rd Generation, SuperDove including coastal blue band and bands interoperable with Sentinel-2 bands.<br>
<i>Date</i>: Date. Date of imagery. <br>
<i> satellite_id</i>: Character. Identification for satellite hardware.<br>
<i>strip_id</i>: Character. Path ID for satellite on that given date.<br>
<i>quality_category</i>: Character. Standard or Test. Images with quality category of Test are unreliable.<br>
<i>clear_confidence_percent</i>: Integer. Confidence score for the entire scene being free from clouds, snow, shadow, haze, etc. This score does not apply to the just clipped portion, so a mask should be applied to the area of interest to determine if the clear_confidence_percent at the pixel level qualifies the image for use.<br>
<i>clear_percent</i>: Integer. Percentage of the entire scene being free from clouds, snow, shadow, haze, etc. This percentage does not apply to the just clipped portion, so a mask should be applied to the area of interest to determine if the udm2's clear mask layer at the pixel level qualifies the image for use.<br>
<i>sun_elevation</i>: The sun's elevation in relation to the satellite's location at the time of acquisition.<br>
<i>sun_azimuth</i>: The sun's azimuth in relation to the satellite's location at the time of acquisition.<br>
<i>view_angle</i>: Numeric. The satellite's angle of capture, measured in degrees from vertical nadir.<br><br>

<b>psa_planet_extractions_conf90_long</b><br>
<i>code.rep</i> Character. Farm code and its replicate feature (1 or 2)<br>
<i>name</i>: Character. Scene name with appended 4-band or 8-band product designation and band name. <br>
<i>value</i>: Numberic. Median band value retained from extractions. Not to be used as the final value.<br>
<i>scene</i>: Character. Scene ID for a given sensor pass. <br>
<i>sensor</i>: Character. Sensor ID for a given satellite. <br>
<i>Band</i>: Band or index (e.g. NDVI) name. <br>
<i>date</i>: Date. Date of imagery. <br>
<i>field.date</i>: Character. code.rep with imagery date appended. Used to calculate mean values for dates with multiple scenes.<br>
<i>bands</i>: Character. 4 or 8 band product.<br>
<i>Scene.Count</i>: Integer. Count of scenes for a given date.<br>
<i>MeanValue</i>: Numeric. Mean value of median values for date with multiple values.<br>
<i>MedianValue</i>: Numeric. Final value that's to represent the code.rep.


<b>RESOURCES</b><br>
Imagery: Planetscope imagery can be accessed from https://g-a69314.7ce1a.03c0.data.globus.org/Imagery/PSA%20Planet/ALL%20IMAGERY.zip<br><br>
Field boundaries: boundaries used for Planet API orders can be accessed from https://g-a69314.7ce1a.03c0.data.globus.org/Imagery/PSA%20Planet/Planet%20Extractions/GIS%20Files%20for%20Orders%20-%20CONFIDENTIAL.zip. These boundaries are different from the original boundaries from Biomass_polygon1.geojson in that they were split into their respective farm codes, buffered out by 30m, and reprojected to WGS84, EPSG 4326.
