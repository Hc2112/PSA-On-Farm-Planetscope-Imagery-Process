# PSA-On-Farm-Planetscope-Imagery-Process
Documented process for ordering, downloading, and processing Planet data extractions across the PSA network. <br><br>

<b>psa_planet_orders_summary.csv<b><br>
A summary of Planetscope surface reflectance imagery (4 or 8 band) that's been acquired for the On-Farm network. Below is its metadata description.<br><br>

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
<


<b>RESOURCES</b><br>
Imagery: Planetscope imagery can be accessed from https://g-a69314.7ce1a.03c0.data.globus.org/Imagery/PSA%20Planet/ALL%20IMAGERY.zip<br><br>
Field boundaries: boundaries used for Planet API orders can be accessed from https://g-a69314.7ce1a.03c0.data.globus.org/Imagery/PSA%20Planet/Planet%20Extractions/GIS%20Files%20for%20Orders%20-%20CONFIDENTIAL.zip
