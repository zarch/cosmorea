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
import argparse

import grass_session as gs

from grass.pygrass.modules import Module, ParallelModuleQueue
from grass.pygrass.gis import Mapset


def get_file_to_import(basedir, file_pat="*.nc"):
    """Return base directory and files matching with pattern"""
    for root, _, files in os.walk(basedir):
        ifiles = fnmatch.filter(files, file_pat)
        for fil in ifiles:
            yield root, fil


def extract_date(rast, datefmt="%Y%m"):
    """Return a tuple containing the basename of te file and the
    datetime instance.

    >>> extract_date("T_2M_201501.nc",
    ...              datefmt="%y%m%d_%H")
    ("T_", datetime.datetime(2015, 1, 1, 0, 0)
    """
    fname, ext = os.path.splitext(rast)
    base, date, hour = fname.split('_')
    dtime = datetime.strptime("{date}_{hour}".format(date=date, hour=hour),
                              datefmt)
    fname = os.path.splitext(rast)[0]
    base =fname[:-7]
    date = fname[-6:]
    dtime = datetime.strptime(date, datefmt)
    return base, dtime


def import2grass(files, gisdbase, location, mapset, datefmt="%Y%m",
                 mapset_fmt="%Y_%m", raster_fmt="%Y_%m",
                 input_fmt="NETCDF:{input_file}",
                 nprocs=4, **kwargs):
    years = []
    env = os.environ.copy()
    mset_envs = {}
    mset_rasters = {}
    queue = ParallelModuleQueue(nprocs=nprocs)
    for fdir, fil in files:
        base, date = extract_date(fil, datefmt=datefmt)
        year = date.year
        if not year in years:
            years.append(year)
        if mapset_fmt:
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
        else:
            menv = gs.grass_init(gs.GISBASE, gisdbase, location, mapset,
                                 env=env.copy())
            mset = Mapset(mapset, location=location, gisdbase=gisdbase)
            rasters = set(mset.glist("raster"))
        rast_name = "{ba}_{da}".format(ba=base, da=date.strftime(raster_fmt))
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
    return years


def main(args):
    # enable compression of NULLs and raster maps
    os.environ["GRASS_COMPRESS_NULLS"] = "1"
    os.environ["GRASS_COMPRESSR"] = "ZSTD"
    os.environ["GRASS_ZLIB_LEVEL"] = "6"
    # set GRASS paths
    GISDBASE = args.grassdata
    LOCATION = args.location
    MAPSET = args.mapset
    # set projection
    create_opts="EPSG:4326"

    # directory containing NetCDF file to import
    BASEDIR = args.INPUT_DIR

    if args.year:
        patt = "*{ye}*.nc".format(ye=args.year)
    else:
        patt = "*.nc"
    if args.variables:
        patt = "{va}{pa}".format(va=args.variables, pa=patt)

    mapsetfmt = None
    if args.ymapset:
        mapsetfmt = "{va}_%Y".format(va=MAPSET)
    elif args.mmapset:
        mapsetfmt = "{va}_%Y_%m".format(va=MAPSET)
    # create a new location if not exists already
    if not os.path.exists(os.path.join(GISDBASE, LOCATION)):
        gs.grass_create(gs.GRASSBIN, os.path.join(GISDBASE, LOCATION),
                        create_opts=create_opts)
    # open a GRASS session in PERMANENT
    with gs.Session(grassbin=gs.GRASSBIN,
                    gisdb=GISDBASE,
                    location=LOCATION,
                    mapset=MAPSET) as sess:
        yrs = import2grass(files=sorted(get_file_to_import(BASEDIR,
                                                           file_pat=patt)),
                                        gisdbase=GISDBASE,
                                        location=LOCATION,
                                        mapset=MAPSET,
                                        mapset_fmt=mapsetfmt,
                                        nprocs=args.nprocs,
                                        memory=args.ram,
                                        title="COSMO REA6: {title}",
                                        flags="o",
                                        overwrite=args.overwrite)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("INPUT_DIR", help="The input directory where to "
                        "NetCDF are stored")
    parser.add_argument("-g", "--grassdata", help="The full path to GRASSDATA",
                        required=True)
    parser.add_argument("-l", "--location", help="The GRASS LOCATION to use",
                        required=True)
    parser.add_argument("-m", "--mapset", default="PERMANENT",
                        help="The GRASS MAPSET to use (default: %(default)s, "
                        "it is not suggested to use this)")
    parser.add_argument("-v", "--variables", help="The selected variables to"
                        " process", nargs='+')
    parser.add_argument("-n", "--nproc", type=int, default=2,
                        help="Processors' number to use (default: %(default)s)")
    parser.add_argument("-y", "--year", type=int, help="Year to analyze")
    parser.add_argument("-r", "--remove", action="store_true",
                        help="Remove NetCDF files")
    parser.add_argument("-Y", "--ymapset", action="store_true",
                        help="Create annual mapset")
    parser.add_argument("-M", "--mmapset", action="store_true",
                        help="Create monthly mapset")
    parser.add_argument("-r", "--ram", default=2048,
                        help="Memory to use to import data")
    parser.add_argument("-o", "--overwrite", action="store_true",
                        help="Set overwrite flag in r.in.gdal")
    parser.add_argument("-t", "--title", help="The title to save in the map's "
                        "history")
    args = parser.parse_args()
    main(args)
