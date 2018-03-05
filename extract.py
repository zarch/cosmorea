#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 09:56:01 2017

@author: pzambelli
"""
import datetime
import fnmatch
import os
import tarfile


BASEDIR = "/share/data/EU/climatic/DWD/COSMO_REA6/T/3D"
FNAME = "{var}_{date:%Y%m%d_%H}.{extension}"


def get_ofile(fname):
    """'T.3D.201501010100.grb'"""
    var, space, date, extension = fname.split(".")
    try:
        info = dict(var=var, space=space, extension=extension,
                    date=datetime.datetime.strptime(date, "%Y%m%d%H%M"))
    except ValueError:
        print(date)
        import pdb; pdb.set_trace()
        datetime.datetime.strptime(date, "%Y%m%d%H%M")
    return FNAME.format(**info)


def get_output_dir(basedir, fname):
    var, space, date, _, _ = fname.split(".")
    info = dict(var=var, space=space,
                date=datetime.datetime.strptime(date, "%Y%m%d"))
    date = info["date"]
    extract_dir = os.path.join(basedir, "extract",
                               date.strftime("%Y"), date.strftime("%m"))
    os.makedirs(extract_dir, exist_ok=True)
    return extract_dir


def extract(ifile, odir, epatterns):
    # ifile: T.3D.20150101.tar.bz2
    # efile: T.3D.201501010100.grb - T.3D.201501012300.grb
    print("Extracting:", ifile)
    _, fname = os.path.split(ifile)
    efile = '.'.join(fname.split('.')[:3]) + '2300.grb'
    ofile = get_ofile(efile)
    opath = os.path.join(odir, ofile)
    if os.path.exists(opath):
        print(f"File {ifile} already extracted: {odir}, skip...")
        return
    with tarfile.open(ifile, mode="r:bz2") as tar:
        members = [m.name for m in tar.getmembers()]
        for efile in fnmatch.filter(members, epatterns):
            ofile = get_ofile(efile)
            opath = os.path.join(odir, ofile)
            print(f"File {efile}: {opath}")
            tar.extract(efile, odir)
            os.rename(os.path.join(odir, efile), opath)


def extract_all(basedir, dpattern, fpattern, epatterns):
    for ddir in sorted(fnmatch.filter(os.listdir(BASEDIR), dpattern)):
        idir = os.path.join(basedir, ddir)
        for cfile in sorted(fnmatch.filter(os.listdir(idir), fpattern)):
            extract_dir = get_output_dir(basedir, cfile)
            extract(os.path.join(idir, cfile), extract_dir, epatterns)


if __name__ == "__main__":
    extract_all(basedir="/share/data/EU/climatic/DWD/COSMO_REA6/T/3D",
                dpattern="h*", fpattern="*.tar.bz2", epatterns="*.grb")
