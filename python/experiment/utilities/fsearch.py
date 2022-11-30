#
# coding=UTF-8
#
# Copyright IBM Inc. 2015 All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Michael Johnston
#
'''Module containing functions for finding all files of a certain type in a directory tree'''

from __future__ import print_function
import sys, os, glob


def ContainingDirectory(filename):

    '''Returns the name of the directory containing filename'''

    dirpath = os.path.split(filename)[0]
    return os.path.split(dirpath)[1]

def FilesInDir(directory, filenames, testFunc, verbose=False):

    '''Given a list of filenames in a directory returns a list containing the paths to those for which testFunc returns True

    Parameters:
        directory - A path to a directoryectory
        filenames - A list of the names of files (note: not paths) that are present in directory

    Returns:
        A list of full paths to any root process mpi_profile files in the supplied list'''

    paths = []
    if verbose is True:
        print('Searching:', directory, file=sys.stderr)
    for filename in filenames:
        if testFunc(filename):
            paths.append(os.path.join(directory, filename))
            if verbose is True:
                print('Found', filename, file=sys.stderr)

    return paths 

def ExpandPath(path):
    
    '''Returns all locations matching path if path is a glob.

    Otherwise just returns path'''

    #Other functions may rely on this function returning path if path is not a glob
    #That is if we pass a path thats not a glob, that also doesn't exist, this function
    #will return an empty list which is contrary to the documented behaviour.
    #That is, glob requires path to exist to work. 

    isGlob = False
    for symbol in ["*", "?", "[", "^"]:
        if path.find(symbol) != -1:
            isGlob = True
            break
    
    if isGlob == True:
        locations = glob.glob(path)
    else:
        locations = [path]

    return locations

def Search(location, testFunc, verbose=False, recurse=True, dirfilter=lambda x: True):

    '''Finds files with specific names based on location

    location can refer to a directory or a file or can be a glob.
    If it refers to a plain filename the method will just return this name in a list.
    If it refers a directory it will be recursively searched by default
    Globs are expanded and all matching files are added and all matchcing directories searched.

    Parameters:
    location - A path to a file or directory - optionaly including wildcards.
           If the path contains wildcards all matching directories are recursively searched.
    testFunc - A function which given a filename returns True if it is the type of file required.
    recurse - If true directories under location (if it is a dir) will be recursively searched
    dirfilter - A function which can take a directory path as an input and returns True if the directory
            should be checked.
            If the function returns False the files in the directory won't be checked and it won't be recursed into

    Returns:
        A list of paths to the files

    Exceptions:
        Raises a ValueError if filename does not exist'''

    #Expand shell globs - 
    locations = ExpandPath(location)

    if verbose:
        if recurse is True:
            print('Will recursively search:', file=sys.stderr)
        else:
            print('Will search:', file=sys.stderr)

        for name in locations:
            print('\t', name, file=sys.stderr)

    files = []  
    for location in locations:
        if not os.path.exists(location):
            raise ValueError('Specified file/directory %s does not exist' % location)

        #If -f is a directory recurse and find all root mpi_profile files we find
        #Otherwise directly add it if its an xml profile
        if os.path.isdir(location): 
            if recurse is True:
                #data[0] is directories, data[2] are the files in the dir
                #data[1] is the subdirs - this is where walk will go next.
                for data in os.walk(location):
                    if dirfilter(data[0]):
                        files.extend(FilesInDir(data[0], data[2], testFunc))
                    else:
                        #Empty the subdir list - walk allows this to be edited in place
                        #so these directories will be skipped
                        data[1][:] = []
            else:
                files.extend(FilesInDir(location, os.listdir(location), testFunc))
        else:
            if testFunc(location):
                files.append(location)
            else:
                print('\tSkipping file %s' % location, file=sys.stderr) 

    return files
