Cleaning.R step by step
author: Bova Marco

We decide to use the datacentermap dataset.  
After inspection and exploratory analysis the following steps are  deemed as necessary: 

1. Filter to observations that DCM says are in the USA.
check for percentage of missing and non missing informations, we can see it's a very low
percentage of NAs, which coincide with SECRET.

2. For the observations that lie within the USA, keep only those whose longitude and 
latitude lie within the Contiguous US OR that have some address information
upload the US map shapefile using lighter 1: 20,000,000 since we are checking for borders. 
Filter for the non contiguos us states. 

3. If an observation has a CONUS longitude/latitude from DCM, keep that coordinate 
as the "best_longitude" and "best_latitude".  This is done because the coordinates 
given by the dataset are considered as the most trustworty.

4.Check for errors in the datacenter, i.e. mismatch between the address labels and the coordinates 
4.1 start from datacenters labeled USA but located outside the conterminous US. 
After pinpointing the ids and coordinates of the mismatched ones, geocode to find 
the correct coordinates of the mismatched datacenters, meaning deriving the coordinates 
from the given address.
4.2  We chose to consider the mismatched observaitons as not reliable hence do not 
repeate the procedure for labeled differently than USA but falling inside the conus. 

To achieve the goal of creating a trustworty dataset it has been chosen to eliminate 
the observations derived from points 4.1 and 4.2 (they are automatically dropped by 
dropping country labels different from USA), easily done since each observation
has a unique id. 

We finally save the dataset, with geospatial informations, named "datacentermap_clean".
This is the main dataset that will be used for future analysis.
The density of the observations and their location is grahically shown in the plots found
in the replication package folder called cleaning. 

It is important to specify that during the cleaning process we have deemed the coordinates
specified in the datacentermap dataset as more reliable than the geocoded. 
This is why in the final version of cleaning we dont consider the possible mismatches
between the stated coordinates and geocoded ones.

5. If an observation only has address info, that concide with the ones with missing 
coordinates values, we cal them secret. Then geocode to get longitude and latitude 
using tidygeocoder and the following three sources: prioritize census > osm > arcgis 
for the "best_longitude" and "best_latitude".
 
The only methodology that does not return only NAs is arcgis,  this is because 
it gives back approximation resulting from entroid-based imputation method in the
absence of complete information. Hence we flag the result by arcgis with a variable
named "arcgis" to be able to identify and discriminate the arcgis computed coordinates easily.

6. Filter these geocoded longitude and latitude observations that lie within CONUS.
Finally create A R data.frame of all data centers in CONUS that have "secretive locations", 
named "secret_dataset". 

7. We have low confidence in the accuracy of this dataset, but it could be useful in case
the analysis need additional datacenters to be carried on.
Since NAs and SECRET coincide and they represent only around 3% of total observation
inside the US, this is why we are confident in dropping them from the main dataset and 
to store the observations in the secret_dataset.

In both dataset the coordinates are names as best_latitude and best_longitude.
