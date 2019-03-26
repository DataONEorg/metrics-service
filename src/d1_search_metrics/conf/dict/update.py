#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib


url = "https://raw.githubusercontent.com/atmire/COUNTER-Robots/master/generated/COUNTER_Robots_list.txt"

content = urllib.urlopen(url)
with open('counter_ua.yml', 'w') as f:
    for item in content:
        item = item.rstrip()
        item = item.replace('\\', '\\\\')
        line = '"' + item + '": counterUA\n'
        f.write(line)
