#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 11:35:32 2017

@author: pzambelli
"""
import asyncio
from asyncio import subprocess
from concurrent.futures import ThreadPoolExecutor
import fnmatch
import os
import argparse
import bz2

GRNAME = "{var}_{date}.grb"
NCNAME = "{var}_{date}.nc"

CDO = ("cdo -f {ffmt} -b {ftype} -O -P {fprocs} -z {fzip} "
       "remapcon,{fgrid} {finput} {foutput}")

FFMT = "nc4c"
FTYPE = "F32"
FZIP = "zip9"


def get_outputs(basedir, fname, sep):
    """"""
    fname, extension = os.path.splitext(fname)
    vals = fname.split(sep)
    if vals[-1] == 'bz2':
        vals = vals[:-1]
    if len(vals) != 4:
        print(f"FNAME: {fname} - {basedir}")
        return "", ""
    var, typ, date = vals[:3]
    conv_dir = os.path.join(basedir, f"{var}")
    os.makedirs(conv_dir, exist_ok=True)
    return conv_dir, GRNAME.format(var=var, date=date), NCNAME.format(var=var, date=date)


@asyncio.coroutine
def convert(queue):
    """"""
    while True:
        cdo, ipath, gpath, opath = yield from queue.get()
        if os.path.exists(opath):
            print(f"File already exists: {opath}")
        else:
            print(f"File {ipath}")
            with open(gpath, 'wb') as grbfile, bz2.open(ipath, 'rb') as file:
                grbfile.write(file.read())
            grbfile.close()
            file.close()
            process = yield from asyncio.create_subprocess_shell(
                cdo, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = yield from process.communicate()
            print(cdo)
            print(stdout)
            print(stderr)
            print("-" * 30)
    return


@asyncio.coroutine
def load_queue(queue, ibasedir, obasedir, fpattern, sep, **copts):
    print("Converting:")
    for base, dirs, files in os.walk(ibasedir):
        for ifile in sorted(fnmatch.filter(files, fpattern)):
            odir, grfile, ofile = get_outputs(obasedir, ifile, sep)
            opath = os.path.join(odir, ofile)
            gpath = os.path.join(base, grfile)
            ipath = os.path.join(base, ifile)
            if not os.path.exists(opath):
                cdo = CDO.format(finput=gpath, foutput=opath, **copts)
                yield from queue.put((cdo, ipath, gpath, opath))
    return


def main(args):
    if args.file:
        FGRID = args.file
    else:
        FGRID = os.path.join(args.OUTPUT_DIR, "grid.txt")
    if not os.path.exists(FGRID):
        raise IOError(f"{FGRID} doesn't exist")
    NUM=12
    if args.year:
        patt = "*{ye}*.grb.bz2".format(ye=args.year)
    else:
        patt = "*.grb.bz2"
    if args.variables:
        for var in args.variables[0].split(','):
            patt = "{va}{pa}".format(va=var, pa=patt)
            queue = asyncio.Queue()
            loop = asyncio.get_event_loop()
            loop.set_default_executor(ThreadPoolExecutor(NUM))
            coros = [asyncio.async(convert(queue)) for i in range(NUM)]
            loop.run_until_complete(load_queue(queue,
                                               ibasedir=args.INPUT_DIR,
                                               obasedir=args.OUTPUT_DIR,
                                               fpattern=patt, sep=args.sep,
                                               ffmt=FFMT, ftype=FTYPE,
                                               fprocs=args.nproc,
                                               fzip=FZIP, fgrid=FGRID))
            loop.run_until_complete(asyncio.wait(coros))
            print("Finished! B-)")
    else:
        queue = asyncio.Queue()
        loop = asyncio.get_event_loop()
        loop.set_default_executor(ThreadPoolExecutor(NUM))
        coros = [asyncio.async(convert(queue)) for i in range(NUM)]
        loop.run_until_complete(load_queue(queue,
                                           ibasedir=args.INPUT_DIR,
                                           obasedir=args.OUTPUT_DIR,
                                           fpattern=patt, sep=args.sep,
                                           ffmt=FFMT, ftype=FTYPE,
                                           fprocs=args.nproc,
                                           fzip=FZIP, fgrid=FGRID))
        loop.run_until_complete(asyncio.wait(coros))
        print("Finished! B-)")
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("INPUT_DIR", help="The input directory where to "
                        "store the downloaded data")
    parser.add_argument("OUTPUT_DIR", help="The output directory where to "
                        "store the downloaded data")
    parser.add_argument("-f", "--file", help="The full path to a txt file "
                        "contains the properties of the new grid.")
    parser.add_argument("-v", "--variables", help="The selected variables to"
                        " process", nargs='+')
    parser.add_argument("-n", "--nproc", type=int, default=2,
                        help="Processors' number to use (default: %(default)s)")
    parser.add_argument("-y", "--year", type=int, help="Year to analyze")
    parser.add_argument("-s", "--sep", default=".",
                        help="The separator used into the file name "
                        "(default: %(default)s)")
    parser.add_argument("-r", "--remove", action="store_true",
                        help="Remove original files")
    args = parser.parse_args()
    main(args)
