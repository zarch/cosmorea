#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 11:35:32 2017

@author: pzambelli
"""
import asyncio
from asyncio import subprocess
from concurrent.futures import ThreadPoolExecutor
import datetime
import fnmatch
import os


IBASEDIR = "/share/data/EU/climatic/DWD/COSMO_REA6/T/3D/extract/"
OBASEDIR = "/share/data/EU/climatic/DWD/COSMO_REA6/T/3D/convert/"

FGRID = os.path.join(OBASEDIR, "new_grid.txt")

FNAME = "{var}_{date:%Y%m%d_%H}.nc"


CDO = ("cdo -f {ffmt} -b {ftype} -O -P {fprocs} -z {fzip} "
       "remapcon,{fgrid} {finput} {foutput}")

FFMT = "nc4c"
FTYPE = "F32"
FPROCS = 4
FZIP = "zip9"
FGRID = os.path.join(OBASEDIR, "new_grid.txt")


def get_outputs(basedir, fname):
    """''"""
    fname, extension = os.path.splitext(fname)
    vals = fname.split("_")
    if len(vals) != 3:
        print(f"FNAME: {fname} - {basedir}")
        return "", ""
    var, date, hour = vals
    # print(var, date, hour)
    info = dict(var=var,
                date=datetime.datetime.strptime(f"{date}_{hour}", "%Y%m%d_%H"))
    date = info["date"]
    conv_dir = os.path.join(basedir,
                            date.strftime("%Y"), date.strftime("%m"))
    os.makedirs(conv_dir, exist_ok=True)
    # info["extension"] = "nc"
    return conv_dir, FNAME.format(**info)



def status_queue(ibasedir, obasedir, fpattern, **copts):
    print("Status:")
    todo = 0
    done = 0
    for base, dirs, files in os.walk(ibasedir):
        for ifile in sorted(fnmatch.filter(files, fpattern)):
            odir, ofile = get_outputs(obasedir, ifile)
            opath = os.path.join(odir, ofile)
            if os.path.exists(opath):
                done += 1
            else:
                todo += 1
    tot = done + todo
    perc = done / tot * 100.
    print(f"done: {done}, misssing: {todo}, total: {tot}, perc: {perc:4.1f}%")
    return done, todo



if __name__ == "__main__":
    status_queue(ibasedir=IBASEDIR, obasedir=OBASEDIR,
                 fpattern="*.grb", ffmt=FFMT,
                 ftype=FTYPE, fprocs=FPROCS,
                 fzip=FZIP, fgrid=FGRID)
