#!/usr/bin/env python
"""\
@file dbbrowser.py
@brief Tools to visualize/edit query profile information

$LicenseInfo:firstyear=2006&license=internal$

Copyright (c) 2006-2010, Linden Research, Inc.

The following source code is PROPRIETARY AND CONFIDENTIAL. Use of
this source code is governed by the Linden Lab Source Code Disclosure
Agreement ("Agreement") previously entered between you and Linden
Lab. By accessing, using, copying, modifying or distributing this
software, you acknowledge that you have been informed of your
obligations under the Agreement and agree to abide by those obligations.

ALL LINDEN LAB SOURCE CODE IS PROVIDED "AS IS." LINDEN LAB MAKES NO
WARRANTIES, EXPRESS, IMPLIED OR OTHERWISE, REGARDING ITS ACCURACY,
COMPLETENESS OR PERFORMANCE.
$/LicenseInfo$
"""

import curses
import os

from indra.base import llsd
from dbmonitor import LLQueryStatMap, asciify

def edit_list(win, l, text):
    # Pop up an edit box to edit a list, and return the resulting list
    # Go forth and edit tables
    curses.textpad.rectangle(win, 2, 2, 13, 63)
    win.addstr(2,3,"Ctrl-G to exit - %s" % text)
    win.refresh()
    editwin = win.derwin(10, 60, 3, 3)
    editwin.clear()
    editwin.refresh()

    clean_list = []
    for line in l:
        res = line.strip()
        if res:
            clean_list.append(res)
    l_str = "\n".join(clean_list)
    editwin.addstr(l_str)
    textbox = curses.textpad.Textbox(editwin)
    res = textbox.edit()
    res = res.strip("\n")
    res_l = []
    for line in res.split("\n"):
        res = line.strip()
        if res:
            res_l.append(res)
    return res_l


class LLQueryMetadata:
    "Stores metadata for all known queries"
    def __init__(self, fn):
        # Keyed on host_clean:query_clean
        self.mMap = {}

        if not os.path.exists(fn):
            # Skip if no metadata file
            return
        # Read in metadata
        query_info_file = open(fn)
        query_info_string = query_info_file.read()
        query_info_file.close()
        query_list = llsd.LLSD.parse(query_info_string)
        if not query_list:
            return
        for query in query_list:
            key = query['host_clean'] + ":" + query['query_clean']
            self.mMap[key] = query
        
    def save(self, fn):
        out = []
        # Sort the output by the key, and format it to allow merging

        sorted_keys = self.mMap.keys()
        sorted_keys.sort()
        for key in sorted_keys:
            out.append(self.mMap[key])
        tmp_fn = "/tmp/query_info.tmp"
        out_file = open(tmp_fn, "w")
        out_file.write(str(llsd.LLSD(out)))
        out_file.close()
        os.system('xmllint --format %s > %s' % (tmp_fn, fn))

    def lookupQuery(self, query):
        key = query.getKey()
        if key in self.mMap:
            return self.mMap[key]
        else:
            return None
        
    def addQuery(self, query):
        key = query.getKey()
        if not key in self.mMap:
            self.mMap[key] = {'host_clean':query.mData['host_clean'], 'query_clean':query.mData['query_clean'],'notes':''}
        return self.mMap[key]

class LLQueryBrowser:
    "Browse through the map of supplied queries."
    def __init__(self, win, query_map = None, metadata = None):
        self.mWin = win
        self.mQueryMap = query_map
        self.mMetadata = metadata
        self.mSortBy = "total_time"
        self.mSortedKeys = query_map.getSortedKeys(self.mSortBy)
        self.mSelectedIndex = 0
        self.mSelectedKey = None
        self.mOffset = 0
        self.mListHeight = 20
        self.mAllowEdit = False
        
    def setQueryMap(self, query_map):
        self.mQueryMap = query_map
        self.mSortedKeys = query_map.getSortedKeys(self.mSortBy)

    def editSelectedText(self, column):
        # Go forth and edit notes
        curses.textpad.rectangle(self.mWin, 2, 2, 13, 63)
        self.mWin.addstr(2,3,"Ctrl-G to exit - Editing %s" % column)
        self.mWin.refresh()
        editwin = self.mWin.derwin(10, 60, 3, 3)
        editwin.clear()
        editwin.refresh()
        query = self.mQueryMap.mQueryMap[self.mSelectedKey]
        query_metadata = self.mMetadata.lookupQuery(query)
        if not query_metadata:
            query_metadata = self.mMetadata.addQuery(query)
        if not query_metadata:
            raise "No query metadata"
        editwin.addstr(query_metadata[column])
        textbox = curses.textpad.Textbox(editwin)
        res = textbox.edit()
        query_metadata[column] = res

    def editSelectedList(self, column):
        query = self.mQueryMap.mQueryMap[self.mSelectedKey]
        query_metadata = self.mMetadata.lookupQuery(query)
        tables = query_metadata[column]
        tables = edit_list(self.mWin, tables, "Editing %s" % column)
        query_metadata[column] = tables

    def handleKey(self, key):
        "Returns True if the key was handled, otherwise false"
        if key == curses.KEY_DOWN:
            self.mSelectedIndex += 1
            self.mSelectedIndex = min(len(self.mSortedKeys)-1, self.mSelectedIndex)
            self.mSelectedKey = self.mSortedKeys[self.mSelectedIndex]

            if self.mSelectedIndex >= self.mOffset + self.mListHeight:
                self.mOffset += 1
            self.mOffset = min(len(self.mSortedKeys)-1, self.mOffset)
        elif key == curses.KEY_NPAGE:
            self.mSelectedIndex += self.mListHeight
            self.mSelectedIndex = min(len(self.mSortedKeys)-1, self.mSelectedIndex)
            self.mSelectedKey = self.mSortedKeys[self.mSelectedIndex]

            self.mOffset += self.mListHeight
            self.mOffset = min(len(self.mSortedKeys)-1, self.mOffset)
        elif key == curses.KEY_UP:
            self.mSelectedIndex -= 1
            self.mSelectedIndex = max(0, self.mSelectedIndex)
            self.mSelectedKey = self.mSortedKeys[self.mSelectedIndex]

            if self.mSelectedIndex < self.mOffset:
                self.mOffset -= 1
            self.mOffset = max(0, self.mOffset)
        elif key == curses.KEY_PPAGE:
            self.mSelectedIndex -= self.mListHeight
            self.mSelectedIndex = max(0, self.mSelectedIndex)
            self.mSelectedIndex = max(0, self.mSelectedIndex)
            self.mSelectedKey = self.mSortedKeys[self.mSelectedIndex]

            self.mOffset -= self.mListHeight
            self.mOffset = max(0, self.mOffset)
        elif key == ord('s'):
            self.toggleSort()
        elif not self.mAllowEdit:
            return False
        elif c == ord('n'):
            # Go forth and edit notes
            self.editSelectedText('notes')
        else:
            return False

        self.redraw()
        return True

    def drawHeader(self, y):
        self.mWin.addstr(y, 0, self.mQueryMap.mDescription + " Query %d of %d" % (self.mSelectedIndex, len(self.mSortedKeys)))
        y += 1
        self.mWin.addstr(y, 0, 'QPS: %.2f\tElapsed: %.2f' % (self.mQueryMap.getQPS(), self.mQueryMap.getElapsedTime()))
        y += 1
        x = 0
        self.mWin.addnstr(y, x, ' TotalSec', 9)
        x += 10
        self.mWin.addnstr(y, x, ' AvgSec', 7)
        x += 8
        self.mWin.addnstr(y, x, '  Count', 7)
        x += 8
        self.mWin.addnstr(y, x, '  QPS', 5)
        x += 6
        self.mWin.addnstr(y, x, 'Host', 9)
        x += 10
        self.mWin.addnstr(y, x, 'Query', 59)
        x += 60
        self.mWin.addnstr(y, x, 'Notes', 10)
        y += 1
        self.mWin.addstr(y, 0, "-"*(x+10))
        y += 1
        return y

    def drawLine(self, y, query, attr, elapsed = 1.0):
        x = 0
        self.mWin.addnstr(y, x, "%9.2f" % (query.mTotalTimeCorrected), 9, attr)
        x += 10
        self.mWin.addnstr(y, x, "%7.4f" % (query.getAvgTimeCorrected()), 7, attr)
        x += 8
        self.mWin.addnstr(y, x, "%7d" % (query.mNumQueriesCorrected), 7, attr)
        x += 8
        self.mWin.addnstr(y, x, "%5.1f" % (query.mNumQueriesCorrected/elapsed), 5, attr)
        x += 6
        self.mWin.addnstr(y, x, query.mData['host_clean'], 9, attr)
        x += 10
        self.mWin.addnstr(y, x, query.mData['query_clean'], 59, attr)
        x += 60
        query_metadata = self.mMetadata.lookupQuery(query)
        if query_metadata:
            self.mWin.addnstr(y, x, query_metadata['notes'], 19, attr)
        
    def drawDetail(self, y, query):
        query.analyze()
        self.mWin.addstr(y, 0, "Tables:")
        self.mWin.addnstr(y, 10, str(query.mData['tables']), 80)
        y += 1
        self.mWin.addstr(y, 0, "Clean Query: " + query.mData['host_clean'])
        y += 1
        self.mWin.addstr(y, 0, asciify(str(query.mData['query_clean'])))
        y += 8
        self.mWin.addstr(y, 0, "Sample Query: " + query.mData['host_clean'])
        y += 1
        try:
            self.mWin.addstr(y, 0, asciify(str(query.mData['query'])))
        except curses.error:
            pass
        y += 8
        query_metadata = self.mMetadata.lookupQuery(query)
        if not query_metadata:
            return
        self.mWin.addstr(y, 0, "Notes:")
        y += 1
        self.mWin.addnstr(y, 0, str(query_metadata['notes']), 80)
        self.mWin.refresh()

    def redraw(self):
        # Find the location of the selected key
        if not self.mSelectedKey:
            self.Selected = 0
            self.mSelectedKey = self.mSortedKeys[0]
            self.mOffset = 0
        else:
            # Find the selected key by brute force for now
            self.mSelectedIndex = -1
            for i in range(0, len(self.mSortedKeys)):
                if self.mSortedKeys[i] == self.mSelectedKey:
                    self.mSelectedIndex = i
                    break
            if -1 == self.mSelectedIndex:
                self.mSelectedIndex = 0
                self.mSelectedKey = self.mSortedKeys[0]

            # Reset the offset to make sure it's on screen
            if self.mSelectedIndex < self.mOffset:
                self.mOffset = self.mSelectedIndex
            elif self.mSelectedIndex >= (self.mOffset + self.mListHeight):
                self.mOffset = (self.mSelectedIndex - self.mListHeight) + 1
        
        # Redraw the display
        self.mWin.clear()
        y = 0
        y = self.drawHeader(y)

        for i in range(self.mOffset, min(len(self.mSortedKeys), self.mOffset + self.mListHeight)):
            attr = curses.A_NORMAL
            if i == self.mSelectedIndex:
                attr = curses.A_BOLD
            self.drawLine(y, self.mQueryMap.mQueryMap[self.mSortedKeys[i]], attr, self.mQueryMap.getElapsedTime())
            y += 1

        # Write detailed information about the selected query
        y += 1
        self.drawDetail(y, self.mQueryMap.mQueryMap[self.mSortedKeys[self.mSelectedIndex]])

    def toggleSort(self):
        "Toggle to the next sort by column"
        if self.mSortBy == "total_time":
            self.mSortBy = "avg_time"
        elif self.mSortBy == "avg_time":
            self.mSortBy = "count"
        elif self.mSortBy == "count":
            self.mSortBy = "total_time"
        self.mSortedKeys = self.mQueryMap.getSortedKeys(self.mSortBy)
        #self.mSelectedIndex = 0
        #self.mSelectedKey = None
        self.mOffset = 0
