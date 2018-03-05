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


@asyncio.coroutine
def convert(queue):
    while True:
        cdo, opath = yield from queue.get()
        if os.path.exists(opath):
            print(f"File already exists: {opath}")
        else:
            print(f"File {opath}")
            process = yield from asyncio.create_subprocess_shell(
                cdo, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = yield from process.communicate()
            print(cdo)
            print(stdout)
            print(stderr)
            print("-" * 30)


@asyncio.coroutine
def load_queue(queue, ibasedir, obasedir, fpattern, **copts):
    print("Converting:")
    for base, dirs, files in os.walk(ibasedir):
        for ifile in sorted(fnmatch.filter(files, fpattern)):
            odir, ofile = get_outputs(obasedir, ifile)
            opath = os.path.join(odir, ofile)
            if not os.path.exists(opath):
                # print(f"File {ifile} already exists: {ofile}")
            #else:
                cdo = CDO.format(finput=os.path.join(base, ifile),
                                 foutput=opath, **copts)
                yield from queue.put((cdo, opath))



if __name__ == "__main__":
    NUM = 12
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(NUM))
    coros = [asyncio.async(convert(queue)) for i in range(NUM)]
    loop.run_until_complete(load_queue(queue,
                                       ibasedir=IBASEDIR, obasedir=OBASEDIR,
                                       fpattern="*.grb", ffmt=FFMT,
                                       ftype=FTYPE, fprocs=FPROCS,
                                       fzip=FZIP, fgrid=FGRID))
    loop.run_until_complete(asyncio.wait(coros))
    print("Finished! B-)")
