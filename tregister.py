#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 06:51:35 2017

@author: pietro
"""
from __future__ import print_function


from datetime import datetime
import os
import argparse

import grass_session as gs

from grass.pygrass.gis import Location

import grass.script as gscript


SEP = {'|': 'pipe',
       ',': 'comma',
       ' ': 'space',
       '\t': 'tab',
       '\n': 'newline'}


def extract_date(rast, datefmt="%Y%m%d_%H"):
    return rast[:5], datetime.strptime(rast[6:], datefmt)


def register(mset_pat, rast_pat,
             datefmt="%Y%m%d_%H", mapset_fmt="r%Y_%m",
             raster_fmt="{base}_{date:%Y%m}",
             sep='|', overwrite=False,
             **kwargs):
    reg_dir = gscript.tempdir()
    env = os.environ.copy()
    loc = Location()
    temporals = {}
    for mset_name in loc.mapsets(pattern=mset_pat):
        menv = gs.grass_init(gs.GISBASE, loc.gisdbase, loc.name,
                             mset_name, env=env.copy())
        for rname in loc[mset_name].glist("raster", pattern=rast_pat):
            base, date = extract_date(rname, datefmt=datefmt)
            trast = raster_fmt.format(base=base, date=date)
            rasts = temporals.get(trast, [])
            rasts.append((rname, "{date:%Y-%m-%d %H:%M:%S}".format(date=date)))
            temporals[trast] = rasts

    # create
    for tname, trasts in temporals.items():
        gscript.run_command("t.create", type="strds", temporaltype="absolute",
                    output=tname, title=tname,
                    description="COSMO REA6: {tname}".format(tname=tname),
                    semantictype="mean", overwrite=overwrite, env=menv)
        csvfile = os.path.join(reg_dir, "{}.csv".format(tname))
        with open(csvfile, mode="w") as csv:
            for row in trasts:
                csv.write(sep.join(row) + '\n')
        gscript.run_command("t.register", overwrite=overwrite,
                    type="raster",  input=tname, file=csvfile,
                    separator=SEP.get(sep, sep), env=menv)


def main(args):
    # enable compression of NULLs and raster maps
    os.environ["GRASS_COMPRESS_NULLS"] = "1"
    os.environ["GRASS_COMPRESSR"] = "ZSTD"
    os.environ["GRASS_ZLIB_LEVEL"] = "6"
    # set GRASS paths
    GISDBASE = args.grassdata
    LOCATION = args.location
    if args.mapset:
        MAPSET = args.mapset
    else:
        MAPSET = "PERMANENT"

    mapsetfmt = None
    if args.ymapset:
        mapsetfmt = "{va}_%Y".format(va=MAPSET)
    elif args.mmapset:
        mapsetfmt = "{va}_%Y_%m".format(va=MAPSET)
    # check if input location exists
    loc_path = os.path.join(GISDBASE, LOCATION)
    if not os.path.exists(loc_path):
        raise TypeError("Input location: {} does not exists".format(loc_path))

    mapsetfmt = None
    if args.ymapset:
        mapsetfmt = "{va}_%Y".format(va=MAPSET)
    elif args.mmapset:
        mapsetfmt = "{va}_%Y_%m".format(va=MAPSET)

    # open a GRASS session in PERMANENT
    with gs.Session(grassbin=gs.GRASSBIN,
                    gisdb=GISDBASE,
                    location=LOCATION,
                    mapset=MAPSET) as sess:
        register(mset_pat="r2015_*", rast_pat="*",
                 datefmt="%Y%m%d_%H", mapset_fmt=mapsetfmt,
                 raster_fmt="%Y_%m_%d_%H",
                 sep='|', overwrite=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    parser.add_argument("-g", "--grassdata", help="The full path to GRASSDATA",
                        required=True)
    parser.add_argument("-l", "--location", help="The GRASS LOCATION to use",
                        required=True)
    parser.add_argument("-m", "--mapset", default="PERMANENT",
                        help="The GRASS MAPSET to use (default: %(default)s, "
                        "it is not suggested to use this)")
    parser.add_argument("-f", "--formats", help="The format of the raster maps"
                        "")
    parser.add_argument("-n", "--nprocs", type=int, default=2,
                        help="Processors' number to use (default: %(default)s)")
    parser.add_argument("-o", "--overwrite", action="store_true",
                        help="Set overwrite")
    parser.add_argument("-Y", "--ymapset", action="store_true",
                        help="Create annual mapset")
    parser.add_argument("-M", "--mmapset", action="store_true",
                        help="Create monthly mapset")
    main(args)