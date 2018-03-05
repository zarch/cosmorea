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
import tempfile
import time


import grass_session as gs

from grass.pygrass.modules import Module, MultiModule, ParallelModuleQueue
from grass.pygrass.gis import Location, Mapset

from grass.script.core import read_command, run_command


SEP = {'|': 'pipe',
       ',': 'comma',
       ' ': 'space',
       '\t': 'tab',
       '\n': 'newline'}


def extract_date(rast, datefmt="%Y%m%d_%H"):
    return rast[:5], datetime.strptime(rast[6:], datefmt)


def register(location, mset_pat, rast_pat,
             datefmt="%Y%m%d_%H", mapset_fmt="r%Y_%m",
             raster_fmt="{base}_{date:%Y%m}",
             sep='|', reg_dir=None, overwrite=False,
             **kwargs):
    reg_dir = tempfile.mkdtemp() if reg_dir is None else reg_dir
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
        run_command("t.create", type="strds", temporaltype="absolute",
                    output=tname, title=tname,
                    description="COSMO REA6: {tname}".format(tname=tname),
                    semantictype="mean", overwrite=overwrite, env=menv)
        csvfile = os.path.join(reg_dir, "{}.csv".format(tname))
        with open(csvfile, mode="w") as csv:
            for row in trasts:
                csv.write(sep.join(row) + '\n')
        run_command("t.register", overwrite=overwrite,
                    type="raster",  input=tname, file=csvfile,
                    separator=SEP.get(sep, sep), env=menv)


if __name__ == "__main__":
    # enable compression of NULLs and raster maps
    os.environ["GRASS_COMPRESS_NULLS"] = "1"
    os.environ["GRASS_COMPRESSR"] = "ZSTD"
    os.environ["GRASS_ZLIB_LEVEL"] = "6"
    # set GRASS paths
    GISDBASE = "/share/data/EU/climatic/DWD/COSMO_REA6/T/3D/grassdb"
    REGISTER = "/share/data/EU/climatic/DWD/COSMO_REA6/T/3D/register"
    LOCATION = "epsg3035"  # output location

    # check if input location exists
    loc_path = os.path.join(GISDBASE, LOCATION)
    if not os.path.exists(loc_path):
        raise TypeError("Input location: {} does not exists".format(loc_path))

    # open a GRASS session in PERMANENT
    with gs.Session(grassbin=gs.GRASSBIN,
                    gisdb=GISDBASE,
                    location=LOCATION,
                    mapset="PERMANENT") as sess:
        register(LOCATION, mset_pat="r2015_*", rast_pat="*",
                 datefmt="%Y%m%d_%H", mapset_fmt="r%Y_%m",
                 raster_fmt="{base}_{date:%Y%m}",
                 sep='|', reg_dir=REGISTER,
                 overwrite=True)
