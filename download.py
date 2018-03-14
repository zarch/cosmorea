#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 11:10:59 2017

@author: pzambelli
"""
import fnmatch
import ftplib
import os
import argparse

DWD_HOST = "ftp-cdc.dwd.de"
COSMO_REA6_DIR = "pub/REA/COSMO_REA6/"
COSMO_REA2_DIR = "pub/reana/COSMO_REA2/"

REA_TIME_LIST = ['constant', 'daily', 'hourly', 'monthly']
REA_TIME = "hourly"
REA_SPACE = "2D"
PVAR = "T_2M"
REA6_2D_VARS = {"CLCT": "total cloud cover",
                "PS": "surface pressure",
                "RELHUM_2M": "relative humidity in 2m",
                "SWDIFDS_RAD": "instantaneous direct radiation",
                "SWDIRS_RAD": "instantaneous diffuse radiation",
                "TOT_PRECIP": "total precipitation",
                "TQV": "total column water vapor content",
                "T_2M": "temperature in 2m",
                "U_10M": "wind velocity at 10 m, u direction",
                "V_10M": "wind velocity at 10 m, v direction",
                "VMAX": "wind gusts at 10m"}
REA6_3D_VARS = {"Q": "water vapor content",
                "T": "temperature",
                "TKE": "turbulent kinetic energy",
                "U": "wind velocity u direction",
                "V": "wind velocity v direction"}
REA2_2D_VARS = {"CLCT": "total cloud cover",
                "HPBL": "height of the boundary layer",
                "PS": "surface pressure",
                "QV3D": "water vapor content",
                "T2M": "temperature in 2m",
                "T3D": "temperature",
                "TKE3D": "turbulent kinetic energy",
                "TOT_PREC": "total precipitation",
                "TQV": "total column water vapor content",
                "U10M": "wind velocity at 10 m, u direction",
                "U3D": "wind velocity u direction",
                "V10M": "wind velocity at 10 m, v direction",
                "V3D": "wind velocity v direction",
                "VMAX10M": "wind gusts at 10m"}
# urllib.urlretrieve
#
#cdc = ftplib.FTP(DWD_HOST)
#cdc.login()
#cdc.cwd('/'.join([COSMO_REA6_DIR, REA_TIME, REA_SPACE]))
#
## physical variables
#pvars = cdc.nlst()
#cdc.cwd(PVAR)
#
#filelist = cdc.nlst()


def download(host, path, pattern, down_dir='download'):
    ftp = ftplib.FTP(host)
    ftp.login()
    try:
        ftp.cwd(path)
    except ftplib.all_errors as e:
        raise e
    flist = fnmatch.filter(ftp.nlst(), pattern)
    flen = len(flist)
    os.makedirs(down_dir, exist_ok=True)
    print("Downloading:")
    for i, fname in enumerate(flist):
        fpath = os.path.join(down_dir, f"{fname}")
        if os.path.exists(fpath):
             print(f"    - {i+1:3d}/{flen:3d}: {fname} {fpath}, already exists!")
        else:
            print(f"    - {i+1:3d}/{flen:3d}: {fname} {fpath}")
            ftp.retrbinary(f"RETR {fname}", open(fpath, 'wb').write)

def main(args):
    if args.rea2:
        mydir = COSMO_REA2_DIR
    elif args.rea6:
        mydir = COSMO_REA6_DIR
    if args.D3:
        typ = "3D"
    else:
        typ = "2D"
    for var in args.variables[0].split(','):
        for year in range(args.endyear, args.startyear - 1, -1):
            if args.rea2:
                mypath = '/'.join([mydir, args.time, typ, var, f"{year}"])
            else:
                mypath = '/'.join([mydir, args.time, typ, var])
            download(host=DWD_HOST, path=mypath,
                     # pattern='T_2M.2D.201512.grb.bz2')
                     pattern='*{ye}*'.format(ye=year),
                     down_dir=args.OUTPUT_DIR)

def print_var(variables):
    for k, v in variables.items():
        print("- {mk}: {mv}".format(mk=k, mv=v))

if __name__ == "__main__":
    #download(host=DWD_HOST,
    #         path='/'.join([COSMO_REA6_DIR, REA_TIME, REA_SPACE, PVAR]),
    #         # pattern='T_2M.2D.201512.grb.bz2')
    #         pattern='T_2M.2D.2015*.grb.bz2',
    #         down_dir='h2015')
    #
    ## r.in.gdal -o -a input=/home/pzambelli/src/hotmaps/hourly_hdd_cdd/T_2M.2D.201512.grb output=T_2M_2D_201512 memory=2047 title=COSMO REA 6km

    # ftp://ftp-cdc.dwd.de/pub/REA/COSMO_REA6/hourly/3D/T/2015/

    parser = argparse.ArgumentParser()
    parser.add_argument("OUTPUT_DIR", help="The output directory where to "
                        "store the downloaded data")
    parser.add_argument("-2", "--rea2", action="store_true",
                        help="Download COSMO REA2 data")
    parser.add_argument("-6", "--rea6", action="store_true",
                        help="Download COSMO REA6 data")
    parser.add_argument("--2D", action="store_true", dest="D2",
                        help="Download COSMO REA 2D variables")
    parser.add_argument("--3D", action="store_true", dest="D3",
                        help="Download COSMO REA 3D variables - 6 lowest model"
                        " levels, approx. 20m to 250m above ground")
    parser.add_argument("-v", "--variables", help="The selected variables to"
                        " download", nargs='+')
    parser.add_argument("-p", "--print", action="store_true",
                        help="Print variables available for selected COSMO REA"
                        " product")
    parser.add_argument("-t", "--time", choices=REA_TIME_LIST),
    parser.add_argument("-s", "--startyear", type=int, default=1995,
                        help="The first year to download")
    parser.add_argument("-e", "--endyear", type=int, default=2005,
                        help="The last year to download")
    args = parser.parse_args()
    if args.print:
        if args.rea2:
            print_var(REA2_2D_VARS)
            parser.exit()
        elif args.rea6:
            print("Variables for 2D:")
            print_var(REA6_2D_VARS)
            print("Variables for 3D:")
            print_var(REA6_3D_VARS)
            parser.exit()
        else:
            raise parser.error("Please select at least one REA product")
    main(args)

