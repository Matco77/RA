#library
library(data.table)
library(tidyverse)
library(sf)
library(readxl)
library(tidygeocoder)
library(geosphere)
library(dplyr)

#Import data
datacenter <-  read_sf("C:\\Users\\Bova\\Downloads\\RAproject\\cleaning\\2507_global_extended_datacentermap_geojson\\datacentermap.geojson")
  
#---- 1.  Filter to observations that DCM says are in the USA
  
datacenter <- datacenter %>% 
  filter(country == "USA"& !state %in% c("Alaska", "Hawaii", "Puerto Rico"))

unique(datacenter$country) #sanity check

#----2. 
'For the observations that lie within the USA, keep only those whose longitude and 
latitude lie within the Contiguous US OR that have some address information using
shapefiles from us census'

#upload the US map shapefile
#use lighter 1: 20,000,000 since we are checking for borders
us_states <- read_sf("C:\\Users\\Bova\\Downloads\\RAproject\\SF census.gov\\cb_2024_us_state_20m.shp")
us_states$NAME #show states contained

# Keep only contiguous United States 
contiguous_us <- us_states %>% 
  filter(!NAME %in% c("Puerto Rico", "Hawaii", "Alaska")) 

# Convert datacenter to spatial dataset
# Coordinate variables are called "longitude" and "latitude
datacenters_sf  <- datacenter %>%
  filter( !(is.na(longitude) | trimws(longitude) == "")) # Filter to observations with nonmissing coordinates

contiguous_us_sf  <- st_transform(contiguous_us , crs = st_crs(datacenters_sf))

# Spatially Join the two sfs
merged <- st_join(datacenters_sf, contiguous_us_sf, join = st_within)

#identify by TRUE the points that fall inside the contiguos us polygon
datacenters_flagged <- merged %>%
  mutate(is_within_us_polygon = !is.na(NAME))

#select only the datacenters that lie within the us contiguos polygon
within_us <- datacenters_flagged %>%
  filter( is_within_us_polygon == T
  )

unique(within_us$country) 
unique(within_us$state)   # sanity checks

#3---
'We prioritize the coordinates given by the dataset as the most trustworty, hence 
If an observation has a CONUS longitude/latitude from DCM, keep that coordinate 
as the "best_longitude" and "best_latitude"'

datacenter_clean <- datacenter %>%
  filter( !(is.na(longitude) | trimws(longitude) == "")) %>% 
  rename(
    best_longitude = longitude,
    best_latitude  = latitude
  )

#4--- check for errors in the datacenter, mismatch btw labelling and coordinates 

#4.1 Datacenters labeled USA but located outside the contiguos US
usa_labeled_outside_conus <- datacenters_flagged %>%
  filter(
    country == "USA",    
    !(is.na(longitude) | trimws(longitude) == ""),
    is_within_us_polygon == FALSE,
    !state %in% c("Alaska", "Hawaii", "Puerto Rico")
  )

unique(usa_labeled_outside_conus$country)  # sanity check

# Get their coordinates
mismatch_coords_matrix <- st_coordinates(usa_labeled_outside_conus)

# Build a summary tibble
usa_mismatch_summary <- tibble(
  id        = usa_labeled_outside_conus$id,
  latitude  = mismatch_coords_matrix[, "Y"],
  longitude = mismatch_coords_matrix[, "X"],
  state     = usa_labeled_outside_conus$state,
  country   = usa_labeled_outside_conus$country
)
#can see that washington is wrong since the longitude is positive and not negative
#Indiana datacenter appears in the correct position even checking the lat and long

#plot it 
usa_labeled_outside_conus %>% 
  ggplot() +
  geom_sf(data = contiguous_us_sf, fill = "lightgray", color = "white") +  # State boundaries
  geom_sf(aes(color = NAME), size = 2) +  # Datacenters colored by state
  labs(
    title = "Mismatched observations" ) +
  theme_minimal()

#geocode to check the position of the datasets 

#get the id of the failures of usa_labeled_outside_conus
id_usa_labeled_outside_conus <- usa_labeled_outside_conus %>% 
  pull(id)

#check the coordinates by geocoding the address
geo_usa_labeled_outside_conus <- datacenter %>%
  filter(id %in% id_usa_labeled_outside_conus) %>% 
  mutate(full_address = paste(address, city, state, sep = ", "))%>%
  geocode(address = full_address,method  = "arcgis",  #faster method use census
          lat = "geocoded_lat",long = "geocoded_lon") 
##
saveRDS(geo_usa_labeled_outside_conus , "geo_usa_labeled_outside_conus .rds") #save it
geo_usa_labeled_outside_conus  <- readRDS("geo_usa_labeled_outside_conus .rds")#load it back

#make it a sf
geo_usa_labeled_outside_sf <- geo_usa_labeled_outside_conus%>% 
  st_as_sf(coords = c("geocoded_lon", "geocoded_lat"), crs = 4326)

geo_usa_labeled_outside_merged <- st_join(geo_usa_labeled_outside_sf, contiguous_us_sf, join = st_within)

#plot the geocoded position
geo_usa_labeled_outside_merged %>% 
  ggplot() +
  geom_sf(data = contiguous_us_sf, fill = "lightgray", color = "white") +
  geom_sf(aes(color = NAME), size = 2) +
  labs(title = "mismatched observations geocoded")+
  theme_minimal() +
  theme(legend.position = "none")

#they appear inside the us, need to change the coords in the original dataset

#4.2 Datacenters *not* labeled USA but that fall inside the conterminous US, are not to
#be considered

#create the dataset dropping the infos of the mismatched observations
datacentermap_clean <- datacenter_clean %>%
  filter(! (id %in% id_usa_labeled_outside_conus)) 

write.csv(
  datacentermap_clean,               
  file = file.path("C:/Users/Bova/Downloads/RAproject/cleaning","datacentermap_clean.csv"),
  row.names = FALSE
)

#plot the density of the datacenters inside the conus based on datacentermap_clean
datacentermap_clean_sf  <- datacentermap_clean %>%
  filter(!is.na(best_longitude), !is.na(best_latitude)) %>% # Filter to observations with nonmissing coordinates
  st_as_sf(coords = c("best_longitude", "best_latitude"), remove = F, crs = 4326)

contiguous_us_sf1<- st_transform(contiguous_us, crs=st_crs(datacentermap_clean_sf))
contiguous_us_sf  <- st_transform(contiguous_us , crs = st_crs(datacenters_sf))

#transform datacentermap_clean_sf as the contiguos_us_sf1
datacentermap_clean_sf <- 
  st_transform(datacentermap_clean_sf, crs = st_crs(contiguous_us_sf1))

merged1 <- st_join(datacentermap_clean_sf, contiguous_us_sf1, join = st_within)

merged1 %>% 
  ggplot() +
  geom_sf(data = contiguous_us_sf1, fill = "lightgray", color = "white") +  # State boundaries
  geom_sf(aes(color = NAME), size = 2) +  # Datacenters colored by state
  labs(
    title = "Datacenters by State",
    color = "State"
  ) +
  theme_minimal()+
  theme(legend.position = "none")

#plot density of datacenters in the contiguos US
merged1 %>%
  filter(!is.na(NAME)) %>%
  mutate(
    lon = st_coordinates(.)[,1],
    lat = st_coordinates(.)[,2]
  ) %>%
  ggplot() +
  geom_sf(data = contiguous_us_sf1, fill = "white", color = "gray90", size = 0.2) +
  stat_density_2d_filled(
    aes(x = lon, y = lat),
    alpha = 0.8,
    contour_var = "count",
    bins = 6
  ) +
  geom_sf(data = contiguous_us_sf1, fill = NA, color = "gray70", size = 0.3) +
  scale_fill_viridis_d(
    name = "Datacenter\nCount",
    option = "cividis"
  ) +
  labs(
    title = "Regional Data Center Distribution",
    subtitle = "Hexagonal density bins show concentration areas across the continental US",
    caption = "Each contour represents areas with similar datacenter density"
  ) +
  theme_void() +
  scale_fill_viridis_d(
    name = "Datacenter\nCount per Region",
    option = "plasma",
    labels = c("0-5", "5-10", "10-15", "15-20", "20-25", "25-30+")
  )+
  theme(
    plot.title = element_text(size = 18, face = "bold"),
    plot.subtitle = element_text(size = 12, color = "gray60", margin = margin(b = 15)),
    legend.position = "right",
    legend.title = element_text(size = 11, face = "bold"),
    plot.caption = element_text(size = 9, color = "gray50"),
    plot.margin = margin(20, 20, 20, 20)
  )


'5. If an observation only has address info, geocode to get longitude and latitude 
using tidygeocoder and the following three sources: prioritize census > osm > arcgis 
for the "best_longitude" and "best_latitude"'

NACoordinates <- datacenter %>%
  filter((is.na(longitude) | trimws(longitude) == "") & country == "USA")
#can notice that all the missing datacenter are SECRET

#geocode to get the coordinates of the observations with na latitude and longitude
secret_census_geocoded <- datacenter %>%
  filter(is.na(latitude) | trimws(longitude) == "") %>%
  mutate(full_address = paste(address, city, sep = ", ")) %>%
  geocode(address = full_address, method = "census",
          lat = "geocoded_lat",long = "geocoded_lon")
colnames(secret_census_geocoded)

###
saveRDS(secret_census_geocoded, "secret_census_geocoded.rds") #save it
secret_census_geocoded <- readRDS("secret_census_geocoded.rds") #to load it back

#check if some missing observations are present
NACoordinates_census <- secret_census_geocoded %>%
  filter(is.na(geocoded_lon)==T | trimws(geocoded_lon) == "")

#geocode the missing coordinates using osm
secret_osm_geocoded <- NACoordinates_census %>%
  mutate(full_address = paste(address, city, sep = ", ")) %>%
  geocode(address = full_address, method = "osm",
          lat = "geocoded_lat",long = "geocoded_lon")
colnames(secret_osm_geocoded)

###
saveRDS(secret_osm_geocoded, "secret_osm_geocoded.rds") #save it
secret_osm_geocoded <- readRDS("secret_osm_geocoded.rds") #to load it back

#check if still some missing observations are present
NACoordinates_osm <- secret_osm_geocoded %>%
  filter(is.na(geocoded_lon...38)==T| trimws(geocoded_lon...38) == "")
 

#geocode the missing observations using arcgis
secret_arcgis_geocoded <- NACoordinates_osm %>%
  mutate(full_address = paste(address, city, sep = ", ")) %>%
  geocode(address = full_address, method = "arcgis",
          lat = "geocoded_lat",long = "geocoded_lon")
colnames(secret_arcgis_geocoded)

###
saveRDS(secret_arcgis_geocoded, "secret_arcgis_geocoded.rds")
secret_osm_geocoded <- readRDS("secret_arcgis_geocoded.rds")

#check if still some missing observations are present
NACoordinates_arcgis <- secret_arcgis_geocoded %>%
  filter(is.na(geocoded_lon...42)==T|  trimws(geocoded_lon...42) == "")

#only arcgis gives back results, this is because as a system it gives back approximate 
#results even when not sure

#rename as best_latitude and best longitude"
secret_geocoded <- secret_arcgis_geocoded %>%
  rename(
    best_longitude = geocoded_lon...42,
    best_latitude = geocoded_lat...41)  %>% 
  select( -geocoded_lat...37,
          -geocoded_lon...38,
          -geocoded_lat...39,
          -geocoded_lon...40, 
          -latitude,
          -longitude,             #drop the Na resulting from previous methods
          -full_address)           

'6. Filter these geocoded,longitude and latitude observations that lie within CONUS'

#take out the missing coordinates
secret_geocoded_sf <- secret_geocoded%>% 
  filter( !(is.na(best_longitude) | trimws(best_longitude) == "")) %>% 
st_as_sf(coords = c("best_longitude", "best_latitude"), crs = 4326, remove = F)

secret_merged<- st_join(secret_geocoded_sf, contiguous_us_sf, join = st_within) 

#take out the one outside the non contiguos us
secret_flagged <- secret_merged %>% 
  mutate(is_within_us_polygon = !is.na(NAME))

secret_validated_inside_us <- secret_flagged %>%
  filter(is_within_us_polygon == TRUE)

unique(secret_validated_inside_us$country) #sanity check

#we get many observations inside the contiguos usa

secret_validated_inside_us %>% 
  ggplot() +
  geom_sf(data = contiguous_us_sf, fill = "lightgray", color = "white") +
  geom_sf(aes(color = NAME), size = 2) +
  labs(title = "secret dataset inside contiguos US")
theme_minimal() +
  theme(legend.position = "none") 

#save the secret geocoded dataset
write.csv(
  secret_validated_inside_us,
  file      = file.path("C:/Users/Bova/Downloads/RAproject/cleaning", "secret_dataset.csv")
)

