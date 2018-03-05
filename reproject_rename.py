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

from grass.pygrass.modules import Module, MultiModule, ParallelModuleQueue
from grass.pygrass.gis import Location, Mapset

from grass.script.core import read_command, run_command


ELEV = {'.1': 10,
        '.2': 35,
        '.3': 69,
        '.4': 116,
        '.5': 178,
        '.6': 258, }


def extract_date(rast, datefmt="%y%m%d_%H"):
    fname, elev = os.path.splitext(rast)
    base, date, hour = fname.split('_')
    dtime = datetime.strptime("{date}_{hour}".format(date=date, hour=hour),
                              datefmt)
    return base, dtime, ELEV[elev]


def reproject(igisdbase, ilocation, olocation, mset_pat, rast_pat,
              datefmt="%Y%m%d_%H", mapset_fmt="r%Y_%m",
              raster_fmt="T{elev:03d}m_%Y%m%d_%H", nprocs=4, ogisdbase=None,
              **kwargs):
    env = os.environ.copy()
    ogisdbase = igisdbase if ogisdbase is None else ogisdbase
    mset_envs = {}
    mset_rasters = {}
    queue = ParallelModuleQueue(nprocs=nprocs)
    iloc = Location(location=ilocation, gisdbase=igisdbase)
    # oloc = Location(location=olocation, gisdbase=ogisdbase)
    #import ipdb; ipdb.set_trace()
    for imset_name in iloc.mapsets(pattern=mset_pat):
        for rname in iloc[imset_name].glist("raster", pattern=rast_pat):
            base, date, elev = extract_date(rname, datefmt=datefmt)
            rast_name = date.strftime(raster_fmt.format(elev=elev))
            mset_name = date.strftime(mapset_fmt)
            mset_path = os.path.join(ogisdbase, olocation, mset_name)
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
                menv = gs.grass_init(gs.GISBASE, ogisdbase, olocation,
                                     mset_name, env=env.copy())
                mset_envs[mset_name] = menv
                mset = Mapset(mset_name, location=olocation, gisdbase=ogisdbase)
                rasters = set(mset.glist("raster"))
                mset_rasters[mset_name] = rasters
                # set region for the mapset
                sregion = read_command("r.proj", location=ilocation,
                                       dbase=igisdbase,
                                       mapset=imset_name,
                                       input=rname, output=rast_name,
                                       flags="g", env=menv)
                #import ipdb; ipdb.set_trace()
                kregion = dict([tuple(s.split('=')) for s in sregion.split()])
                run_command("g.region", save=mset_name, env=menv,
                            overwrite=True, **kregion)
                menv["WIND_OVERRIDE"] = mset_name

            if rast_name not in rasters:
                mod = Module("r.proj", location=ilocation, dbase=igisdbase,
                             mapset=imset_name, input=rname,
                             output=rast_name,
                             run_=False, **kwargs)
                mod.env_ = menv
                print(rast_name)
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
    ILOCATION = "wgs84"     # input location
    OLOCATION = "epsg3035"  # output location
    # set projection
    create_opts="EPSG:3035"

    # check if input location exists
    iloc_path = os.path.join(GISDBASE, ILOCATION)
    if not os.path.exists(iloc_path):
        raise TypeError("Input location: {} does not exists".format(iloc_path))

    # create a new location if not exists already
    oloc_path = os.path.join(GISDBASE, OLOCATION)
    if not os.path.exists(oloc_path):
        gs.grass_create(gs.GRASSBIN, os.path.join(GISDBASE, OLOCATION),
                        create_opts=create_opts)

    # open a GRASS session in PERMANENT
    with gs.Session(grassbin=gs.GRASSBIN,
                    gisdb=GISDBASE,
                    location=OLOCATION,
                    mapset="PERMANENT") as sess:
        reproject(igisdbase=GISDBASE,
                  ilocation=ILOCATION, olocation=OLOCATION,
                  mset_pat="o2015_*", rast_pat="T_201501*",
                  datefmt="%Y%m%d_%H", mapset_fmt="r%Y_%m",
                  raster_fmt="T{elev:03d}m_%Y%m%d_%H", nprocs=12,
                  memory=2000, method="nearest",
                  overwrite=True)
