# cosmorea

It is a set of tools to:
* download,
* reproject into WGS84
* import into a GRASS GIS location
* reproject from the GRASS GIS WGS84 into a EPSG:3035
* create a strs temporal data set

From the COSMO REA {6|2} data set.

Everything at this stage is experimental and to be considered as proof of concept.
The tools must be modify to be a set of shell scripts with no hard-coded paths.

## requirements

The tools required Python >= 3.7, GRASS GIS, grass_session library, and cdo to convert and read the grib format.


## approach

The general approach is to have specific command lines that can be executed in parallel,
So you can start downloading the files, start reprojecting, and importing into a GRASS GIS location all in parallel.
