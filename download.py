#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 11:10:59 2017

@author: pzambelli
"""
import fnmatch
import ftplib
import os

DWD_HOST = "ftp-cdc.dwd.de"
COSMO_REA6_DIR = "pub/REA/COSMO_REA6/"
COSMO_REA2_DIR = "pub/REA/COSMO_REA2/"

REA_TIME_LIST = ['constant', 'daily', 'hourly', 'monthly']
REA_TIME = "hourly"
REA_SPACE = "2D"
PVAR = "T_2M"

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
    ftp.cwd(path)
    flist = fnmatch.filter(ftp.nlst(), pattern)
    flen = len(flist)
    os.makedirs(down_dir, exist_ok=True)
    print("Downloading:")
    for i, fname in enumerate(flist):
        fpath = os.path.join(down_dir, f"{fname}")
        if os.path.exists(fpath):
             print(f"    - {i+1:3d}/{flen:3d}: {fname} ({fpath}, already exists!")
        else:
            print(f"    - {i+1:3d}/{flen:3d}: {fname} ({fpath}")
            ftp.retrbinary(f"RETR {fname}", open(fpath, 'wb').write)


if __name__ == "__main__":
    #download(host=DWD_HOST,
    #         path='/'.join([COSMO_REA6_DIR, REA_TIME, REA_SPACE, PVAR]),
    #         # pattern='T_2M.2D.201512.grb.bz2')
    #         pattern='T_2M.2D.2015*.grb.bz2',
    #         down_dir='h2015')
    #
    ## r.in.gdal -o -a input=/home/pzambelli/src/hotmaps/hourly_hdd_cdd/T_2M.2D.201512.grb output=T_2M_2D_201512 memory=2047 title=COSMO REA 6km

    # ftp://ftp-cdc.dwd.de/pub/REA/COSMO_REA6/hourly/3D/T/2015/
    year = 2015
    for year in range(2015, 2009, -1):
        download(host=DWD_HOST,
                 path='/'.join([COSMO_REA6_DIR, REA_TIME, "3D", "T", f"{year}"]),
                 # pattern='T_2M.2D.201512.grb.bz2')
                 pattern='T.3D.20*.tar.bz2',
                 down_dir="/".join(["/share/data/EU/climatic/DWD/COSMO_REA6/T/3D",
                                    f'h{year}']))
