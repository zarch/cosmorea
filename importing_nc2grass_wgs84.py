#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 06:51:35 2017

@author: pietro
"""
from __future__ import print_function


from datetime import datetime
from datetime import timedelta
import fnmatch
import sys
import os
import socket
import argparse
from copy import deepcopy
from subprocess import PIPE
import grass_session as gs
from grass.pygrass.modules import Module, ParallelModuleQueue
from grass.pygrass.gis import Mapset
from grass.exceptions import CalledModuleError
#in seconds
MINUTE = 3600

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
    fname = os.path.splitext(rast)[0]
    base =fname[:-7]
    date = fname[-6:]
    dtime = datetime.strptime(date, datefmt)
    return base, dtime


def rename_maps(base, date=None, year=None, month=None, startnum=1, log=None):
    if isinstance(date, datetime):
        mydat = deepcopy(date)
    elif year and month:
        mydat = datetime(year, month, 1, 0, 0)
    else:
        print("Please set date or year (with or without month")
        sys.exit(1)
    if not date:
        date = datetime(year, month, 1, 0, 0)
    if log:
        fi = open(log, 'w')
    for do in range(startnum, 745):
        cop = Module("g.rename", raster=("{ba}_{mo}.{im}".format(ba=base,
                                                                 mo=date.strftime("%Y_%m"),
                                                                 im=do),
                                         "{ba}_{da}".format(ba=base,
                                                            da=mydat.strftime("%Y_%m_%d_%H"))),
                     stdout_=PIPE, stderr_=PIPE)
        if log:
            if cop.outputs.stdout:
                fi.write("{}\n".format(cop.outputs.stdout))
            if cop.outputs.stderr:
                fi.write("{}\n".format(cop.outputs.stderr))
        mydat = mydat + timedelta(seconds=MINUTE)
    if log:
        fi.close()


def convert_maps(base, date=None, year=None, month=None, startnum=1, log=None):
    """Convert the data if needed, like temperature from kelvin to celsius"""
    if isinstance(date, datetime):
        mydat = deepcopy(date)
    elif year and month:
        mydat = datetime(year, month, 1, 0, 0)
    else:
        print("Please set date or year with or without month")
        sys.exit(1)
    if not date:
        date = datetime(year, month, 1, 0, 0)
    if log:
        fi = open(log, 'w')
    Module("g.region", raster="{ba}_{mo}.{im}".format(ba=base, im=startnum,
                                                      mo=date.strftime("%Y_%m")))
    for do in range(startnum, 745):
        out = "{ba}_{da}".format(ba=base, da=mydat.strftime("%Y_%m_%d_%H"))
        inn = "{ba}_{mo}.{im}".format(ba=base, mo=date.strftime("%Y_%m"),
                                      im=do)
        if base == 'T_2M':
            try:
                mapc = Module("r.mapcalc", expression="{ou} = {inn} - "
                              "273.15".format(ou=out, inn=inn),
                              stdout_=PIPE, stderr_=PIPE)
                if log:
                    if mapc.outputs.stdout:
                        fi.write("{}\n".format(mapc.outputs.stdout))
                    if mapc.outputs.stderr:
                        fi.write("{}\n".format(mapc.outputs.stderr))
                Module("g.remove", type="raster", name=inn, flags="f")
            except CalledModuleError:
                continue
        if base == 'TOT_PRECIP':
            try:
                mapc = Module("r.mapcalc", expression="{ou} = if({inn} < 0, 0,"
                              " {inn})".format(ou=out, inn=inn),
                              stdout_=PIPE, stderr_=PIPE)
                if log:
                    if mapc.outputs.stdout:
                        fi.write("{}\n".format(mapc.outputs.stdout))
                    if mapc.outputs.stderr:
                        fi.write("{}\n".format(mapc.outputs.stderr))
                Module("g.remove", type="raster", name=inn, flags="f")
            except CalledModuleError:
                continue
        mydat = mydat + timedelta(seconds=3600)
    if log:
        fi.close()


def import2grass(files, args, datefmt="%Y%m", mapset_fmt="%Y_%m",
                 raster_fmt="%Y_%m", input_fmt="NETCDF:{input_file}",
                 **kwargs):
    # old variables
    nprocs = args.nprocs
    gisdbase = args.grassdata
    location = args.location
    mapset = args.mapset
    rename = args.rename
    convert = args.convert
    outs = {}
    env = os.environ.copy()
    mset_envs = {}
    mset_rasters = {}
    if nprocs > 1:
        queue = ParallelModuleQueue(nprocs=nprocs)

    for fdir, fil in files:
        base, date = extract_date(fil, datefmt=datefmt)
        if base not in outs.keys():
            outs[base] = []
        else:
            outs[base].append(date)
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
            mod = Module("r.in.gdal", quiet=True,
                         input=input_fmt.format(input_file=ifile),
                         output=rast_name, run_=False, **kwargs)
            if nprocs > 1:
                mod.env_ = menv
                #time.sleep(0.2) # sllep otherwise there is a problem in creating
                queue.put(mod)
            else:
                mod.run()
                if convert:
                    convert_maps(base, date, log=args.log)
                if rename:
                    rename_maps(base, date, log=args.log)
    if nprocs > 1:
        queue.wait()
    return outs


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

    if args.year and args.month:
        patt = "*{ye}*{mo}.nc".format(ye=args.year,
                                      mo=str(args.month).zfill(2))
    elif args.year:
        patt = "*{ye}*.nc".format(ye=args.year)
    elif args.month and not args.year:
        print("'month' option requires also 'year' option")
        sys.exit(1)
    else:
        patt = "*.nc"

    mapsetfmt = None
    if args.ymapset:
        mapsetfmt = "{va}_%Y".format(va=MAPSET)
    elif args.mmapset:
        mapsetfmt = "{va}_%Y_%m".format(va=MAPSET)

    title = "COSMO REA6"
    if args.title:
        title = "{pr}: {ti}".format(pr=title, ti=args.title)
    # create a new location if not exists already
    if not os.path.exists(os.path.join(GISDBASE, LOCATION)):
        gs.grass_create(gs.GRASSBIN, os.path.join(GISDBASE, LOCATION),
                        create_opts=create_opts)
    # open a GRASS session in PERMANENT
    with gs.Session(grassbin=gs.GRASSBIN,
                    gisdb=GISDBASE,
                    location=LOCATION,
                    mapset=MAPSET) as sess:
        if args.variables:
            for varia in args.variables.split(','):
                patt = "{va}{pa}".format(va=args.varia, pa=patt)
                fils = sorted(get_file_to_import(BASEDIR, file_pat=patt))
                if not args.title:
                    title = "{pr}: {ti}".format(pr=title, ti=varia)

                yrs = import2grass(files=fils,
                                   args=args,
                                   mapset_fmt=mapsetfmt,
                                   memory=args.ram,
                                   title=title,
                                   flags="o",
                                   overwrite=args.overwrite)
        else:
            fils = sorted(get_file_to_import(BASEDIR, file_pat=patt))
            yrs = import2grass(files=fils,
                               args=args,
                               mapset_fmt=mapsetfmt,
                               memory=args.ram,
                               title=title,
                               flags="o",
                               overwrite=args.overwrite)
        # rename the maps if required by the user
        if args.nprocs > 1 and args.rename:
            for bas, dates in yrs.items():
                for dat in dates:
                    rename_maps(bas, dat, log=args.log)
        elif args.nprocs > 1 and args.convert:
            for bas, dates in yrs.items():
                for dat in dates:
                    convert_maps(bas, dat, log=args.log)


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
                        " process, comma separated", nargs='+')
    parser.add_argument("-n", "--nprocs", type=int, default=2,
                        help="Processors' number to use, 1 for singular run "
                        "without ParallelModuleQueue (default: %(default)s)")
    parser.add_argument("-y", "--year", type=int, help="Year to analyze")
    parser.add_argument("-t", "--month", type=int, help="Month to analyze")
    parser.add_argument("-r", "--remove", action="store_true",
                        help="Remove NetCDF files")
    parser.add_argument("-Y", "--ymapset", action="store_true",
                        help="Create annual mapset")
    parser.add_argument("-M", "--mmapset", action="store_true",
                        help="Create monthly mapset")
    parser.add_argument("-R", "--ram", default=2048,
                        help="Memory to use to import data")
    parser.add_argument("-o", "--overwrite", action="store_true",
                        help="Set overwrite flag in r.in.gdal")
    parser.add_argument("-T", "--title", help="The title to save in the map's "
                        "history")
    parser.add_argument("-w", "--rename", action="store_true",
                        help="Rename the maps with date and time")
    parser.add_argument("-c", "--convert", action="store_true",
                        help="Convert data if needed and also rename the maps")
    parser.add_argument("-L", "--log", help="The path for the log file")
    args = parser.parse_args()
    main(args)
