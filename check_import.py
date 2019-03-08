#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon May 14 14:01:55 2018

@author: lucadelu
"""
import sys
from datetime import datetime
from datetime import timedelta
from copy import deepcopy
from subprocess import PIPE
import argparse
import calendar
import grass_session as gs

from grass.pygrass.modules import Module
from grass.exceptions import CalledModuleError

MINUTE = 3600

MONTHS = {1: 744, 2: 672, 3: 744, 4: 720, 5: 744, 6: 720, 7: 744, 8: 744,
          9: 720, 10: 744, 11: 720, 12: 744}

def check_maps(base, date=None, year=None, month=None, log=None):
    if isinstance(date, datetime):
        mydat = deepcopy(date)
    elif year and month:
        mydat = datetime(year, month, 1, 0, 0)
    else:
        print("Please set date or year with or without month")
        sys.exit(1)
    if mydat.month == 2:
        if calendar.isleap(mydat.year):
            counts = 696
        else:
            counts = MONTHS[mydat.month]
    else:
        counts = MONTHS[mydat.month]
    glist = Module("g.list", type="raster", mapset=".",
                   pattern="{ba}_{da}_*".format(ba=base,
                                                da=mydat.strftime("%Y_%m")),
                   stdout_=PIPE, stderr_=PIPE)
    lists = glist.outputs.stdout.splitlines()
    if len(lists) == counts:
        outlog = "{ye} {mo} correct".format(ye=mydat.year, mo=mydat.month)
        if log:
            log.write(outlog)
        return True
    else:
        outlog = "ERROR {ye} {mo} non correct".format(ye=mydat.year,
                                                      mo=mydat.month)
        if log:
            log.write(outlog)
        else:
            print(outlog)
        return False


def fix_maps_missing_first(base, startdate, shift=3600):
    if isinstance(startdate, datetime):
        mydat = deepcopy(startdate)
    glist = Module("g.list", type="raster", mapset=".",
                   pattern="{ba}_{da}_*".format(ba=base,
                                                da=mydat.strftime("%Y_%m")),
                   stdout_=PIPE, stderr_=PIPE)
    lists = glist.outputs.stdout.splitlines()
    for mapp in reversed(lists):
        out = "{ba}_{da}".format(ba=base, da=mydat.strftime("%Y_%m_%d_%H"))
        try:
            cop = Module("g.rename", raster=(mapp, out))
        except CalledModuleError:
            print("ERROR: not possible rename {im} in {om}".format(im=mapp,
                                                                   om=out))
            continue
        mydat = mydat - timedelta(seconds=shift)


def main()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-g", "--grassdata", help="The full path to GRASSDATA",
                        required=True)
    parser.add_argument("-l", "--location", help="The GRASS LOCATION to use",
                        required=True)
    parser.add_argument("-m", "--mapset", default="PERMANENT",
                        help="The GRASS MAPSET to use (default: %(default)s, "
                        "it is not suggested to use this)")
    parser.add_argument("-v", "--variables", help="The selected variables to"
                        " process, comma separated", nargs='+')
    parser.add_argument("-n", "--nprocs", type=int, default=2,
                        help="Processors' number to use, 1 for singular run "
                        "without ParallelModuleQueue (default: %(default)s)")
    parser.add_argument("-y", "--year", type=int, help="Year to analyze")
    parser.add_argument("-t", "--month", type=int, help="Month to analyze")
    parser.add_argument("-L", "--log", help="The path for the log file")
    args = parser.parse_args()
    main(args)