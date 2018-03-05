#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 18 12:41:30 2017

@author: pzambelli
"""

import asyncio
import aioftp



DWD_HOST = "ftp-cdc.dwd.de"
COSMO_REA6_DIR = "pub/REA/COSMO_REA6/"
COSMO_REA2_DIR = "pub/REA/COSMO_REA2/"

REA_TIME_LIST = ['constant', 'daily', 'hourly', 'monthly']
REA_TIME = "hourly"
REA_SPACE = "2D"
PVAR = "T_2M"

PATH = '/'.join([COSMO_REA6_DIR, REA_TIME, REA_SPACE])


async def get_mp3(host, port, login, password):
    async with aioftp.ClientSession(host, port, login, password) as client:
        for path, info in (await client.list(recursive=True)):
            if info["type"] == "file" and path.suffix == ".mp3":
                await client.download(path)


async def get_grid(host, ftppath):
    async with aioftp.ClientSession(host) as client:
        for path, info in (await client.list(ftppath)):
            if info["type"] == "file" and path.suffix == ".mp3":
                print(path.name)
                await client.download(path)




loop = asyncio.get_event_loop()
tasks = (
    #get_mp3("server1.com", 21, "login", "password")),
    #get_mp3("server2.com", 21, "login", "password")),
    #get_mp3("server3.com", 21, "login", "password")),
    get_grid(host=DWD_HOST, ftppath=PATH)
)
loop.run_until_complete(asyncio.wait(tasks))
loop.close()