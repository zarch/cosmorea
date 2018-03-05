#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 11:35:32 2017

@author: pzambelli
"""
import asyncio
from asyncio import subprocess
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
    var, date, hour = fname.split("_")
    # print(var, date, hour)
    info = dict(var=var,
                date=datetime.datetime.strptime(f"{date}_{hour}", "%Y%m%d_%H"))
    date = info["date"]
    conv_dir = os.path.join(basedir,
                            date.strftime("%Y"), date.strftime("%m"))
    os.makedirs(conv_dir, exist_ok=True)
    # info["extension"] = "nc"
    return conv_dir, FNAME.format(**info)


asyncio.coroutine
def convert_all(ibasedir, obasedir, fpattern, **copts):
    print("Converting:")
    for base, dirs, files in os.walk(ibasedir):
        for ifile in sorted(fnmatch.filter(files, fpattern)):
            odir, ofile = get_outputs(obasedir, ifile)
            opath = os.path.join(odir, ofile)
            if os.path.exists(opath):
                print(f"File {ifile} already exists: {ofile}")
            else:
                print(f"File {ifile}: {ofile}")
                cdo = CDO.format(finput=os.path.join(base, ifile),
                                 foutput=opath, **copts)
                print(cdo)
                process = yield from asyncio.create_subprocess_shell(
                    cdo, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                stdout, stderr = yield from process.communicate()
                print(stdout)
                print(stderr)
            print("-" * 30)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(convert_all(ibasedir=IBASEDIR, obasedir=OBASEDIR,
                                        fpattern="*.grb", ffmt=FFMT,
                                        ftype=FTYPE, fprocs=FPROCS,
                                        fzip=FZIP, fgrid=FGRID))
