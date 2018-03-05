#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 06:51:35 2017

@author: pietro
"""
from __future__ import print_function


from datetime import datetime
import fnmatch
import os
import socket
import time


import grass_session as gs

from grass.pygrass.modules import Module, ParallelModuleQueue
from grass.pygrass.gis import Location, Mapset


def get_file_to_import(basedir, file_pat="*.nc"):
    for root, _, files in os.walk(basedir):
        ifiles = fnmatch.filter(files, file_pat)
        for fil in ifiles:
            yield root, fil

def get_files_to_reproject(gisdbase, location, pattern):
    loc = Location()


def extract_date(rast, datefmt="%y%m%d_%H"):
    """Return a tuple containing the basename of te file and the
    datetime instance.

    >>> extract_date("/share/data/EU/climatic/DWD/COSMO_REA6/T/3D"
    ...              "/convert/2015/01/T_20150131_11.nc",
    ...              datefmt="%y%m%d_%H")
    ("T_", datetime.datetime(2017, 12, 31, 11, 00)
    """
    fname, ext = os.path.splitext(rast)
    base, date, hour = fname.split('_')
    dtime = datetime.strptime("{date}_{hour}".format(date=date, hour=hour),
                              datefmt)
    return base, dtime


def import2grass(files, gisdbase, location, datefmt="%Y%m%d_%H",
                 mapset_fmt="%Y_%m", raster_fmt="%Y%m%d_%H",
                 input_fmt="NETCDF:{input_file}",
                 nprocs=4, **kwargs):
    env = os.environ.copy()
    mset_envs = {}
    mset_rasters = {}
    queue = ParallelModuleQueue(nprocs=nprocs)
    for fdir, fil in files:
        base, date = extract_date(fil, datefmt=datefmt)
        mset_name = date.strftime(mapset_fmt)
        mset_path = os.path.join(gisdbase, location, mset_name)
        if not os.path.exists(mset_path):
            gs.grass_create(gs.GRASSBIN, mset_path, create_opts="")
            try:
                os.makedirs(os.path.join(mset_path, '.tmp'))
                os.makedirs(os.path.join(mset_path, '.tmp',
                                         socket.gethostname()))
            except:
                # ignore error in creating the
                pass
        try:
            menv = mset_envs[mset_name]
            rasters = mset_rasters[mset_name]
        except KeyError:
            menv = gs.grass_init(gs.GISBASE, gisdbase, location, mset_name,
                                 env=env.copy())
            mset_envs[mset_name] = menv
            mset = Mapset(mset_name, location=location, gisdbase=gisdbase)
            rasters = set(mset.glist("raster"))
            mset_rasters[mset_name] = rasters
        rast_name = date.strftime(raster_fmt)
        if rast_name + '.1' not in rasters or rast_name + '.6' not in rasters:
            ifile = os.path.join(fdir, fil)
            mod = Module("r.in.gdal",
                         input=input_fmt.format(input_file=ifile),
                         output=rast_name, run_=False, **kwargs)
            mod.env_ = menv
            print(rast_name, ifile)
            #time.sleep(0.2) # sllep otherwise there is a problem in creating
            queue.put(mod)
    queue.wait()


if __name__ == "__main__":
    # enable compression of NULLs and raster maps
    os.environ["GRASS_COMPRESS_NULLS"] = "1"
    os.environ["GRASS_COMPRESSR"] = "ZSTD"
    os.environ["GRASS_ZLIB_LEVEL"] = "6"
    # set GRASS paths
    GISDBASE = "/share/data/EU/climatic/DWD/COSMO_REA6/T/3D/grassdb"
    LOCATION = "wgs84"
    # set projection
    create_opts="EPSG:4326"

    # directory containing NetCDF file to import
    BASEDIR = "/share/data/EU/climatic/DWD/COSMO_REA6/T/3D/convert/2015"

    # create a new location if not exists already
    if not os.path.exists(os.path.join(GISDBASE, LOCATION)):
        gs.grass_create(gs.GRASSBIN, os.path.join(GISDBASE, LOCATION),
                        create_opts=create_opts)
    # open a GRASS session in PERMANENT
    with gs.Session(grassbin=gs.GRASSBIN,
                    gisdb=GISDBASE,
                    location=LOCATION,
                    mapset="PERMANENT") as sess:
        import2grass(files=sorted(get_file_to_import(BASEDIR,
                                                     file_pat="T_2015*.nc")),
                     gisdbase=GISDBASE,
                     location=LOCATION,
                     datefmt="%Y%m%d_%H",
                     mapset_fmt="o%Y_%m",
                     raster_fmt="T_%Y%m%d_%H",
                     nprocs=12,
                     input_fmt="NETCDF:{input_file}",
                     memory=2040,
                     title="COSMO REA6: Air Temperature",
                     flags="o",
                     overwrite=True)
