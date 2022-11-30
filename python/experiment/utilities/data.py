#
# coding=UTF-8
#
# Copyright IBM Inc. 2015 All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Author(s): Vassilis Vassiliadis, Michael Johnston

#
'''Module containing useful data classes 

Also contains some functions which act like class initialisers.'''
from __future__ import print_function

from . import cmp
from future import standard_library
from functools import reduce, cmp_to_key
standard_library.install_aliases()
from builtins import str as newstr
from builtins import zip
from builtins import range

from builtins import object
import pickle, csv, io, operator, datetime
import sys
import os
import collections

listConvert = tuple
try:
    import numpy
    listConvert = numpy.asarray
except ImportError as error:
    print('Numpy not found - column arrays will be returned as tuples', file=sys.stderr)

def ParseHeaderPath(headerPath, headers=None):

    '''Parses a header path

    A header path is a string of the form $columnHeader.$propertyPath

    $propertyPath is a python property access string (e.g. database.tables.names)

    This method identifies and returns the $columnHeader and $propertyPath

    Parameters:
        headerPath: A string containing a header path
        headers: A list containing the headers.
            Required to correctly identify column headers which include periods e.g. "No. Ranks"
            If None, all periods are assumed to denote property accesses

    Returns:
        A tuple containing the columnHeader and the property path path

    Note: If headers is supplied and no match is found, the function behaves as if no headers were supplied.
    It is up to the caller to determine the validity of the returned value

    If multiple headers match the first match is used.

    Exceptions:
        Raises AttributeError if headerPath is not a string
    '''

    header = None
    if headers is not None:
        try:
            matches = [h for h in headers if headerPath[:len(h)] == h]
        except TypeError:
            #Normally if you try to use [] on an object without __getitem__ it raises
            #an AttributeError (no attribute __getitem__)
            #Except if its an int or float in which case TypeError is raised??
            raise AttributeError("Supplied header path object does not support indexing")

        if len(matches) == 1:
            header = matches[0]

    if header is not None:
        #+1 to remove first period after header
        path = headerPath[len(header)+1:]
    else:
        attributes = headerPath.split('.')
        header = attributes[:1][0]
        path = '.'.join(attributes[1:])

    return header, path

def split(func, sequence):

    '''Splits sequence based on func

    Parameters:
        func - A function taking a single argument and returning True or False
        The elements of sequence are passed to this function
        sequence - An iterable sequence

    Returns:
        Two lists.
        Elements for which func is True are returned in the first
        Elements for which func is False are returned in  the other'''

    tseq = []
    fseq = []

    for el in sequence:
        if func(el):
            tseq.append(el)
        else:
            fseq.append(el)

    return tseq, fseq

class CouldNotDetermineCSVDialectError(Exception):

    '''Raised when a supposed CSV file's format could not be determined'''

    pass


def matrixFromFile(filename):

    '''Creates a matrix instance from one stored in a file.

    Parameters
        filename: The name of a file with a archived Matrix instance in it

    Return
        Returns a Matrix instance

    Exceptions:
        Raises an IOError if no file called filename exists
        Raises a TypeError if filename does not contain an archived matrix'''

    file = open(filename)
    unpickler = pickle.Unpickler(file)
    matrix = unpickler.load()
    file.close()

    if not isinstance(matrix, Matrix):
        raise TypeError("Unarchived object is not an instance of the Matrix class")

    return matrix

def matrixFromCSVRepresentation(string, readHeaders=False,
                                dialect=None, name='No Name', checkHeaders=True, **formatingArgs):

    '''Creates a matrix instance from a csv representation of a matrix (a string).

    Note: Any lines beginning with a hash (#) are ignored.
    Note: If the file doesn't use a quite standard CSV format dynamic determination
    of header existence is prone to be random.
    Minor variations in a file will result in a header being found or not

    Parameters
        string: The csv reprensentation
        readHeaders: True if the first (non-comment) line of the string should be interpreted
            as giving the column headers. Default is False.
            Overrides checkHeaders if True
        checkHeaders: Tries to automatically determine if there are headers
        dialect: An instance of the csv.Dialect class containing information
            on the dialect of the csv representation. Defaults to None.
            In this case the function tries to deduce the dialect itself.
        formattingArgs: An alternative to the dialect parameter.
            Any options after checkHeaders are gathered into a dictionary and passed to the reader
            The following options are valid:
            delimiter, doublequote,  escapechar, lineterminator, quotechar, quoting, skipinitialspace, strict

    Return
        Returns a Matrix instance

    Exceptions:
        Raises a TypeError if a matrix could not be read from string
        Raises a ValueError is string is empty'''

    return Matrix.matrixFromCSVRepresentation(string,
                                              readHeaders=readHeaders,
                                              dialect=dialect,
                                              name=name,
                                              checkHeaders=checkHeaders,
                                              **formatingArgs)

def matrixFromCSVFile(filename, readHeaders=False, dialect=None, name=None, checkHeaders=True, **formattingArgs):

    '''Creates a matrix instance from a csv file.

    Note: Any lines beginning with a hash  (#) are ignored

    Parameters
        filename: The name of a file with a matrix in csv format
        readHeaders: True if the first (non-comment) line of the string should be interpreted
            as giving the column headers. Default is False.
            Overrides checkHeaders if True
        checkHeaders: If True tries to automatically determine if there are headers
        dialect: An instance of the csv.Dialect class containing information
        on the dialect of the csv file. Defaults to None.
        In this case the function tries to deduce the dialect itself.
        name: The name to be given to the created matrix

    Return
        Returns a Matrix instance

    Exceptions:
        Raises an IOError if no file called filename exists
        Raises a TypeError if a matrix could not be read from the file'''

    return Matrix.matrixFromCSVFile(
        filename,
        readHeaders=readHeaders,
        dialect=dialect,
        name=name,
        checkHeaders=checkHeaders,
        **formattingArgs
        )

class Matrix(object):

    '''Basic matrix class

    Can store heterogenous data and contains functionality for handling named columns.
    Also supports comparison and iteration and can be converted to other formats'''

    @classmethod
    def matrixFromCSVFile(cls, filename, readHeaders=False, dialect=None, name=None, checkHeaders=True, **formattingArgs):

        '''Creates a matrix instance from a csv file.

        Note: Any lines beginning with a hash  (#) are ignored

        Parameters
            filename: The name of a file with a matrix in csv format
            readHeaders: True if the first (non-comment) line of the string should be interpreted
                as giving the column headers. Default is False.
                Overrides checkHeaders if True
            checkHeaders: If True tries to automatically determine if there are headers
            dialect: An instance of the csv.Dialect class containing information
            on the dialect of the csv file. Defaults to None.
            In this case the function tries to deduce the dialect itself.
            name: The name to be given to the created matrix

        Return
            Returns a Matrix instance

        Exceptions:
            Raises an IOError if no file called filename exists
            Raises a TypeError if a matrix could not be read from the file'''

        # Open the file and read the contents
        # Then pass it to matrixFromCSVRepresentation
        fileObject = open(filename, 'r')

        # mm = mmap.mmap(fileObject.fileno(), 0, mmap.MAP_PRIVATE, mmap.PROT_READ)
        # m =  matrixFromCSVRepresentation(mm, readHeaders, dialect)
        # mm.close()

        string = fileObject.read()
        fileObject.close()
        if name is None:
            path, name = os.path.split(filename)
            name = os.path.join(os.path.split(path)[1], name)

        m = cls.matrixFromCSVRepresentation(string,
                                        readHeaders=readHeaders,
                                        dialect=dialect,
                                        name=name,
                                        checkHeaders=checkHeaders,
                                        **formattingArgs)

        return m


    @classmethod
    def matrixFromCSVRepresentation(cls, string, readHeaders=False,
                                        dialect=None, name='No Name', checkHeaders=True, **formatingArgs):

        '''Creates a matrix instance from a csv representation of a matrix (a string).

        Note: Any lines beginning with a hash (#) are ignored.
        Note: If the file doesn't use a quite standard CSV format dynamic determination
        of header existence is prone to be random.
        Minor variations in a file will result in a header being found or not

        Parameters
            string: The csv representation
            readHeaders: True if the first (non-comment) line of the string should be interpreted
                as giving the column headers. Default is False.
                Overrides checkHeaders if True
            checkHeaders: Tries to automatically determine if there are headers
            dialect: An instance of the csv.Dialect class containing information
                on the dialect of the csv representation. Defaults to None.
                In this case the function tries to deduce the dialect itself.
            formattingArgs: An alternative to the dialect parameter.
                Any options after checkHeaders are gathered into a dictionary and passed to the reader
                The following options are valid:
                delimiter, doublequote,  escapechar, lineterminator, quotechar, quoting, skipinitialspace, strict

        Return
            Returns a Matrix instance

        Exceptions:
            Raises a TypeError if a matrix could not be read from string
            Raises a ValueError is string is empty'''


        #Have to ensure string is in unicode
        string = string.encode(encoding='utf-8').decode('utf-8')
        b = datetime.datetime.now()

        if len(string) == 0:
            raise ValueError('String is empty - cannot create matrix')

        # Find the first line that isn't a # into a string.
        # Then use StringIO to treat this string like a file
        # for use with the csv reader object.
        inputFile = io.StringIO(string)

        # Seek to the first non-empty line that does not begin with a hash

        position = 0
        flag = 0
        while flag == 0:
            line = inputFile.readline()
            if len(line) != 0 and line[0] != '#':
                flag = 1
            elif line == '':
                raise ValueError('File only contains comments  - cannot create matrix')
            else:
                position = inputFile.tell()

        inputFile.seek(position)
        fileContent = inputFile.read()
        inputFile.close()

        # Create the file-like string object
        csvFile = io.StringIO(fileContent)

        # Check the file dialect before processing
        if dialect is None and not formatingArgs:
            line = csvFile.readline()
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(line)
                # print 'Dialect: ', dialect
                # print "Delimiter: '%s'" % dialect.delimiter
                # print r"Terminator: '%s'" % dialect.lineterminator
                # print "Skip initial space:", dialect.skipinitialspace
            except csv.Error as error:
                print(error, name)
                raise CouldNotDetermineCSVDialectError

            #
            # Before: readHeaders is False, checkHeaders is True. (DEFAULT)
            # Action: Dynamically check for headers. Afterwards readHeaders may be True or False
            #
            # Before: readHeaders is True. checkHeaders is True OR False
            # Action: No dynamic check. Reading headers is forced
            #
            # Before readHeaders is False. checkHeaders is False
            # Action: No dynamic check. No headers will be read
            if checkHeaders and not readHeaders:
                try:
                    line = line + csvFile.readline() + csvFile.readline()
                    readHeaders = sniffer.has_header(line)
                except csv.Error as error:
                    print('Error while determining header existence:', error)
                    # print error, name

                    # print 'Result of dynamic header check:', readHeaders

        csvFile.seek(0)

        if dialect is not None:
            # This prevents spaces after delimters being interpreted as elements
            # This should prevent blank elements being parsed
            dialect.skipinitialspace = True

        if not 'skipinitialspace' in list(formatingArgs.keys()):
            formatingArgs['skipinitialspace'] = True

        # The reader reads the csv file and converts each
        # line of the file into a list containing the csv elements.
        reader = csv.reader(csvFile, dialect=dialect, **formatingArgs)

        # Get the column headers if specified
        headers= None
        try:
            if readHeaders:
                headers = next(reader)
        except StopIteration as error:
            print("No valid csv data found.", error, file=sys.stderr)

        # NOTE: Assuming all data in a column is of the same type ...
        # This is problematic e.g. if 0 is written to first element in a column but rest of entries are floats
        # FIMXE: Make optional
        # Test for conversions
        conversions = {'int': [], 'float': []}
        rows = []
        try:
            row = next(reader)
            rows.append(row)
            for i in range(len(row)):
                try:
                    # Check if its an int
                    # Do this first since all ints can be converted to floats
                    row[i] = int(row[i])
                    conversions['int'].append(i)
                    continue
                except ValueError:
                    pass

                try:
                    # Check if its a float
                    row[i] = float(row[i])
                    conversions['float'].append(i)
                except ValueError:
                    # Its not an int or float
                    pass
        except StopIteration as error:
            print(error)
            pass

        integers = conversions['int']
        floats = conversions['float']

        try:
            switchColumns = []
            for row in reader:
                if len(row) != 0:
                    addRow = True
                    for index in integers:
                        try:
                            row[index] = int(row[index])
                        except:
                            # Error in conversion - assume we can convert to float
                            # Switch this col to a float for subsequent conversions
                            try:
                                row[index] = float(row[index])
                            except ValueError:
                                # See comment in float coversions below regardin next 3 lines
                                if index == 0 and row[index].strip()[0] == '#':
                                    print('Found comment in CSV body - %s - ignoring' % (" ".join(row)), file=sys.stderr)
                                    addRow = False

                            else:
                                switchColumns.append(index)

                    if addRow:
                        for index in floats:
                            try:
                                row[index] = float(row[index])
                            except ValueError:
                                # Could not convert to float which means its probably a string
                                # Check if its a comment - index = 0 and starts with a hash
                                # If so remove this row
                                # Otherwise leave as string
                                if index == 0 and row[index].strip()[0] == '#':
                                    print('Found comment in CSV body - %s - ignoring' % (" ".join(row)), file=sys.stderr)
                                    addRow = False

                    if addRow:
                        for index in switchColumns:
                            loc = integers.index(index)
                            integers.pop(loc)
                            floats.append(index)
                            #print >> sys.stderr, 'Switched column %d to float' % index

                        rows.append(row)

                    if len(switchColumns) != 0:
                        switchColumns[:] = []

        except ValueError as error:
            print('Encountered error converting data in CSV rep', error, file=sys.stderr)

        csvFile.close()

        if headers is not None:
            headers = [header.strip() for header in headers]

        matrix = cls(rows=rows, headers=headers, name=name)

        return matrix


    def __init__(self, rows=[], headers=None, name='No Name'):

        '''Initialise a new matrix

        Parameters -
            rows: A list of lists with the contents of the matrix.
                All lists must be the same length otherwise a TypeError exception is raised
            headers: A list containing the headers for each column
            name: A name for the matrix

        Exceptions -
            The number of headers must be equal to the number of elements in each list in rows.
            If not a TypeError is raised
            A TypeError is also raised is the supplied matrix name is not a string'''

        self.numberRows = len(rows)
        self.setName(name)
        self.columnHash = None
        self.currentKeyColumn = None

        if len(rows) != 0:
            self.numberColumns = len(rows[0])
        else:
            self.numberColumns = 0

        #Check length of each row is the same
        for row in rows:
            if len(row) != self.numberColumns:
                raise IndexError('All supplied rows not of same length (%d, %d, %s)' % (self.numberColumns, len(row), str(row)))

        #Check header length
        if headers is not None:
            if len(headers) != self.numberColumns:
                #Its seems relatively common in space separated files output by Fortran programs
                #for the last header string to be followed by spaces,
                #but numeric values in the table body to not be
                #This causes the header to have an extra empty element, ''
                #Check for this here
                if headers[-1] == '' and (len(headers) - self.numberColumns) == 1:
                    headers.pop(-1)
                else:
                    print('First row:', rows[0], file=sys.stderr)
                    print('Headers:', headers, file=sys.stderr)
                    raise IndexError('Incorrect number of column headers supplied (%d, %d)' % (self.numberColumns, len(headers)))

            self.headers = list(headers)
        else:
            self.headers = []
            for i in range(self.numberOfColumns()):
                self.headers.append('None')

        #Copy the rows
        self.matrix = []
        for row in rows:
            newRow = []
            for element in row:
                newRow.append(element)
            self.matrix.append(newRow)

    def __cmp__(self, object):

        '''Compares two matrix instances.

        Return
            Returns 0 if the data in the matrices ns identical and they both have identical headers.
            Otherwise returns -1 or 1'''

        if isinstance(object, Matrix):
            #Cmp returns 0 if two objects are equal
            value = cmp(self.matrix, object.matrix) or cmp(self.columnHeaders(), object.columnHeaders())
            value = value or cmp(self.name(), object.name())
            #Value will be either 1 or 0 at this point.
            return value
        else:
            return -1

    def __getRowSlice(self, obj):

        '''Processes the row slice'''

        start, end, step = obj.indices(self.numberOfRows())
        rows = []
        for i in range(start, end):
            rows.append(self.row(i))
            i = i + step
        return self.__class__(rows=rows, headers=self.columnHeaders(), name=self.name())

    def __getColumnSlice(self, obj):

        '''Processes the slice'''

        start, end, step = obj.indices(self.numberOfColumns())
        matrix = self.__class__(rows=[], name=self.name())
        for i in range(start, end):
            matrix.addColumn(self.column(i))
            i = i + step

        matrix.setColumnHeaders(self.columnHeaders()[start:end:step])

        return matrix

    def __getitem__(self, obj):

        '''For matrix[i] returns row i
        For matrix[i:j] return another Core.Matrix instance with the specified rows'''

        if type(obj) is slice:
            return self.__getRowSlice(obj)
        elif type(obj) is tuple and len(obj) == 2:
            #Assume first slice is row slice and second is column slice
            #First get the row slice
            matrix = self.__getRowSlice(obj[0])
            #Now get a column slice of the row slice
            return matrix.__getColumnSlice(obj[1])
        else:
            return self.row(obj)

    def __str__(self):

        '''Returns an information string'''

        if self.numberOfRows() == 0:
            string = "Empty Matrix"
        else:
            string = ("Matrix with %d rows and %d columns - %d elements in total"  %
                    (self.numberOfRows(), self.numberOfColumns(),
                    self.numberOfRows()*self.numberOfColumns()))

        return string

    def __getattr__(self, name):

        '''Allows column headers to be used as attributes

        Check if name is a column header - if it is return it.
        Otherwise raise AttributeError.

        If the column name has space you can access it by converting it to camelCase.
        In this case the name is assumed to have all capitals'''

        #Don't search for headers attribute if it hasn't been defined!
        #Otherwise start an infinite recursion
        if name == 'headers':
            raise AttributeError(name)

        if name in self.headers:
            return self.columnWithHeader(name)
        elif name[:2] != "__":
            #try converting the name from camel case
            indexes = [0]
            for i in range(len(name)):
                if name[i].isupper() and i != 0:
                    indexes.append(i)

            indexes.append(len(name))
            substrings = []
            for i in range(len(indexes) - 1):
                substring = name[indexes[i]:indexes[i+1]]
                substring = substring.capitalize()
                substrings.append(substring)

            convertedNameSpaces = " ".join(substrings)
            convertedNameNoSpaces = "".join(substrings)
            if convertedNameSpaces in self.headers:
                return self.columnWithHeader(convertedNameSpaces)
            elif convertedNameNoSpaces in self.headers:
                return self.columnWithHeader(convertedNameNoSpaces)
            else:
                raise AttributeError(name)
        else:
            raise AttributeError(name)

    def name(self):

        '''Returns the matrices names'''

        return self.matrixName

    def setName(self, name):

        '''Sets the matrices name

        Parameters:
            name - a string

        Exceptions:
            Raises a TypeError if name is not a string'''

        #On 2.7 str will be a byte string and newstr a unicode string
        #On 3.7 they will be the same
        if not isinstance(name, str) and not isinstance(name, newstr):
            raise TypeError("Matrix name must be a string object")

        self.matrixName = name

    def numberOfRows(self):

        '''Returns the number of rows in the matrix'''

        return self.numberRows

    def numberOfColumns(self):

        '''Returns the number of columns in the receiver'''

        return self.numberColumns

    def element(self, rowIndex, columnIndex):

        '''Returns a matrix element

        Parameters
            rowIndex: The elements rowIndex
            columnIndex: The elements columnIndex

        Exceptions:
            Raises an IndexError if either rowIndex or columnIndex is out of the range of the reciever'''

        return self.matrix[rowIndex][columnIndex]

    def row(self, index):

        '''Returns a row of the matrix

        Parameters
            index: an int indicating the row to return

        Return
            A tuple containing the row elements.

        Exceptions:
            Raises an IndexError of index is out of range of the receiver'''

        return tuple(self.matrix[index])

    def column(self, label):

        '''Returns a row of the matrix

        Parameters
            lable: an int or column header or header path indicating the column to return

        A header path is a string used to extract attributes from objects in a column header
        Syntax: $columnHeader.$attribute

        Return
            A numpy.array (or a tuple) containing the column elements

        Exceptions:
            Raises an IndexError of index is out of range of the receiver
            IndexError is also raised if label is a non-existant header'''

        try:
            #Will raise AttributeError if label is not a string
            header, attributePath = ParseHeaderPath(label, self.columnHeaders())
            index = self.indexOfColumnWithHeader(header)
        except AttributeError:
            #Assume its an integer and catch below if issues
            index = label
            attributePath = None
            pass

        try:
            column = [row[index] for row in self.matrix]
        except IndexError:
            raise IndexError('Column index %d is out of range of the receiver (%d)' % (index, self.numberOfColumns()))
        except TypeError:
            raise TypeError('Specified label (%s) does not match any column header' % label)

        if attributePath is not None and len(attributePath) != 0:
            column = [getattr(el, attributePath) for el in column]

        #This will be a numpy array or a tuple depending on whats available
        return listConvert(column)

    def columns(self, columns, matrix=True):

        '''Returns the given columns as a matrix

        Params:
            columns: A list of column indices and/or headers
            matrix: If True a matrix is returned containing the given columns.
                If False a list of tuples is returned

        For the purposes of iterating over the values both methods are the same
        e.g. for a,b,c in matrix.columns(a,b,c):

        '''

        d = list(zip(*[self.column(c) for c in columns]))
        retval = d
        if matrix:
           retval =  self.__class__(rows=d,
                      headers=columns,
                      name=self.name())

        return retval

    def addElements(self, data, rowIndex=-1, createNewColumns=True):

        '''As set elements but creates a new row.

        If the matrix is not empty the row is initialised using addEmptyRow and the filled with setElements

        If the matrix is empty a row is added with len(data) elements and the headers are set to data.keys()

        NOTE: For the purposes of creating new columns all keys in the data parameter
        are interpreted as columnHeaders.
        If a key is an index and createNewColumns is True a new column will be created with header "index".

        Parameters:
            data: A dictionary of column header: data pairs
            rowIndex: The index at which to insert the new data
            createNewColumns: If True new columns are create for headers in data which are not in the receiver.
                The columns are initialised with None
        '''

        if self.numberOfColumns() == 0:
            self.addRow([0]*len(data))
            self.setColumnHeaders(list(data.keys()))
        else:
            self.addEmptyRow(rowIndex=rowIndex)

        #Create new columns for new headers in data.keys()
        if createNewColumns:
            newCols = [x for x in list(data.keys()) if x not in self.columnHeaders()]
            for h in newCols:
                self.addColumn([None]*self.numberOfRows(), header=h)

        self.setElements(data, rowIndex=rowIndex)

    def setElements(self, data, rowIndex=-1):

        '''Sets the elements in rowIndex with the contents of data.

        Parameters:
            rowIndex: An int. The index of the row to set the data in
            data: A dictionary of columnHeader/Index : data pairs

        Exceptions: Raises AttributeError if a column header or Index doesn't exist.
        No data is set if this occurs
        '''

        #Get and check indexes
        indexes = []
        values = []
        for label in list(data.keys()):
            try:
                index = self.indexOfColumnWithHeader(label)
            except ValueError:
                index = label

            try:
                firstElement = self.matrix[0][index]
            except IndexError:
                raise AttributeError('Column index %s is out of range of the receiver (%d)' % (
                            index, self.numberOfColumns()))
            except TypeError:
                raise AttributeError('Specified label (%s) does not match any column header' % label)
            else:
                indexes.append(index)
                values.append(data[label])

        #Now have a map of columnIndex:data pairs
        for columnIndex, value in zip(indexes,values):
            self.matrix[rowIndex][columnIndex] = value

    def getElements(self, rowIndex=-1, headers=None):

        '''Returns the data in a row as a dictionary of columnHeader:rowValue pairs

        Parameters:
            rowIndex: int. The row to get data from
            headers: list. A list of column-headers to extract data from.
                Defaults to all.
                Note: Indexes cannot currently be used

        Returns:
                A dictionary of header:row-value pairs.

        Exceptions:
            IndexError if rowIndex is beyond range of receiver
            ValueError if no column matching a header can be found'''

        if headers is None:
            headers = self.columnHeaders()

        r = collections.OrderedDict()
        for h in headers:
            r[h] = self.matrix[rowIndex][self.indexOfColumnWithHeader(h)]

        return r

    def addEmptyRow(self, rowIndex=-1):

        '''Adds a row of zeros to the  dataframe'''

        self.addRow([0]*self.numberOfColumns(), index=rowIndex)

    def addRow(self, row, index=-1):

        '''Adds a row to the receiver

        Parameters:
            aList: A list containing the elements to add
                Must containing the correct number of elements (if matrix not empty)
            index: The index at which to add the new row.
                -1 indicates it should be placed at the end.

        Exceptions
            Raises an IndexError if index is beyond the range of the receiver and is not -1
            Raises an IndexError if aList does not containing the correct number of elements.'''

        if self.numberOfColumns() != 0:
            if len(row) != self.numberOfColumns():
                raise IndexError('Incorrect number of elements (%d) in supplied row %d' % (len(row), self.numberOfRows()))

            if index != -1:
                if index > self.numberOfRows():
                    raise IndexError('Supplied index %d is beyond the range of the receiver %d' % (index, self.numberOfRows()))
                else:
                    self.matrix.insert(index, list(row))
            else:
                self.matrix.append(list(row))
        else:
            self.matrix.append(list(row))
            self.numberColumns = len(row)
            self.setColumnHeaders(['None']*self.numberOfColumns())

        self.numberRows = self.numberRows + 1

        #Update column hash if present
        if self.keyColumn() is not None:
            self._createColumnHash(self.keyColumn())

    def addColumn(self, column, index=-1, header='None'):

        '''Adds a column to the receiver

        The new column has header 'None'

        Parameters:
            aList: A list containing the elements to add
                Must containing the correct number of elements (unless adding to empty matrix)
            index: The index at which to add the new column.
                -1 indicates it should be placed at the end.

        Exceptions
            Raises an IndexError if index is beyond the range of the receiver.
            Raises an IndexError if aList does not containing the correct number of elements.'''

        if self.numberOfRows() != 0:
            if len(column) != self.numberOfRows():
                raise IndexError('Incorrect number of elements (%d) in supplied column (%d)' % (len(column), self.numberOfRows()))

            #If index is -1 change to the number of columns in the receiver
            if index == -1:
                index = self.numberOfColumns()

            if index > self.numberOfColumns():
                raise IndexError('Supplied index %d is beyond the range of the receiver %d' % (index, self.numberOfColumns()))
            elif self.numberOfRows() == 0:
                for i in range(len(column)):
                    self.matrix.append([])
                self.numberRows = len(column)

            for i in range(len(column)):
                self.matrix[i].insert(index, column[i])
        else:
            index = 0
            for i in range(len(column)):
                self.matrix.append([column[i]])

            self.numberRows = len(column)

        #Add a new header
        self.headers.insert(index, header)
        self.numberColumns = self.numberColumns + 1

    def removeRows(self, index):

        '''Removes the specified row

        Parameters:
            index - The index of the row to remove.
            A negative index means remove from the end'''

        if index < 0:
            index = self.numberOfRows() + index

        if abs(index) > self.numberOfRows():
            raise IndexError('Supplied index %d is beyond the range of the receiver %d' % (index, self.numberOfColumns()))


        self.matrix.pop(index)
        self.numberRows = self.numberRows - 1

        #Update column hash if present
        if self.keyColumn() is not None:
            self._createColumnHash(self.keyColumn())

    def removeColumn(self, index):

        '''Removes the specified column

        Parameters:
            index - The index of the column to remove.
            A negative index means remove from the end'''

        if index < 0:
            index = self.numberOfColumns() - index

        if abs(index) > self.numberOfColumns():
            raise IndexError('Supplied index %d is beyond the range of the receiver %d' % (index, self.numberOfColumns()))

        self.headers.pop(index)
        for i in range(self.numberOfRows()):
            self.matrix[i].pop(index)

        self.numberColumns = self.numberColumns - 1

    def columnWithHeader(self, aString):

        '''Returns the column whose header is given by aString.

        Parameters
            header: A aString

        Return
            A tuple containing the column elements.
            If more than one column has the same header the first is returned.
            None if no column has aString as a header'''

        try:
            index = self.indexOfColumnWithHeader(aString)
            column = self.column(index)
        except ValueError:
            #No header called aString exists
            column = None

        return column

    def headerForColumn(self, index):

        '''Returns the header for a specified column.

        Parameters
            index: The index of a column.

        Return
            The column header - a aString.

        Exceptions
            Raises an IndexError if index is out of range of the receiver'''

        return self.headers[index]

    def columnHeaders(self):

        '''Returns the headers of all the columns

        Return
            A tuple containing the column headers'''

        return tuple(self.headers)

    def indexOfColumnWithHeader(self, aString):

        '''Returns the index of the column whose header is equal to aString

        Parameters
            aString: A string corresponding to a column header
                Can also be a header path

        Exceptions:
            Raises an ValueError if aString is not the header of any column'''

        try:
            header, path = ParseHeaderPath(aString, self.columnHeaders())
            index =  self.headers.index(header)
        except (ValueError, AttributeError) as error:
            raise ValueError('Unknown header %s' % aString)

        return index

    def setColumnHeader(self, index, header):

        '''Sets a new header for the specified column

        Parameters
            index: The index of the column whose header is to be changed.
            header: A string - The new header

        Exceptions
            Raises an IndexError if index is out of range of the receiver'''

        self.headers[index] = header

    def setColumnHeaders(self, headers):

        '''Sets new headers for all the columns.

        Parameters
            headers - A list or tuple containing the new values.
                The first value in this variable if set as the header for the first column and so on.

        Exceptions
            Raises an IndexError if headers does not have the correct number of elements.
            It must have the same number of elements as rows'''

        if len(headers) != self.numberOfColumns():
            raise IndexError("Incorrect number of headers supplied %d. Requires %d" % (len(headers), self.numberOfColumns()))

        self.headers = list(headers)

    def description(self):

        '''Returns a description of the receiver (a string)'''

        return self.__str__()

    def writeToFile(self, filename):

        '''Writes the matrix to a file.

        The matrix is written in a binary representation.
        Use matrixFromFile() to read the matrix.

        Parameters
            filename: Name of the file to write to.

        Exceptions
            Raises an IOError if the file cannot be created'''

        file = open(filename, 'w')
        pickler = pickle.Pickler(file)
        pickler.dump(self)
        file.close()

    def csvRepresentation(self, sep=", ", comments=None, includeHeaders=True):

        '''Returns a csv representation of the receiver.

        This is a aString with row elements seperated by commas
        and rows separated by newlines

        Parameters:
            comments: A list of strings which will be inserted as comments in the returned string.
            The strings should not contain new line characters.
            includeHeaders: If false the headers are not present in the CSV representation.
            Default: True'''

        csvRep = ""
        if includeHeaders:
            csvRep = sep.join([str(x) for x in (self.columnHeaders() or [])])
            csvRep = csvRep + "\n"

        rowStrings = []
        for row in self:
            if len(row) > 1:
                rowStrings.append(sep.join([str(x) for x in (row or [])]))
            else:
                rowStrings.append(str(row[0]))

        csvRep = csvRep + '\n'.join(rowStrings) + '\n' if csvRep.strip() != "" else csvRep.strip()

        if comments is not None and len(comments) != 0:
            commentString = ""
            for comment in comments:
                #Get rid of any newlines as these could affect the formatting
                comment = comment.replace("\n", " ")
                commentString = commentString + "# " + comment + '\n'
            csvRep = commentString + csvRep

        return csvRep

    def sort(self, columnHeader='Total', descending=True, function=cmp):

        '''Sorts the entire matrix in place by the data the column with header'columnHeader'.

        Params:
            columnHeader: The name of the column to sort

        Returns: Nothing'''

        index = self.indexOfColumnWithHeader(columnHeader)

        userfunc = cmp_to_key(function)
        self.matrix.sort(key=lambda x: userfunc(x[index]), reverse=descending)

    def filter(self, columnHeader='Total', filterFunction=None, sort=False):

        '''Returns a matrix filtered using filterFunction on the given column

        Parameters:
            columnHeader - One of the matrices in the receiver. Defaults to 'Total'
            filterFunction - The function to filter using. Must return True or False for each value in the choosen column

        Returns:
            A Matrix instance or None if no rows pass the filter

        Exceptions:
            Raises a ValueError if there is no column with the given header'''

        filteredRows = []
        index = self.indexOfColumnWithHeader(columnHeader)

        for row in self:
            value = row[index]
            if filterFunction(value):
                filteredRows.append(row)
        if sort:
            filteredRows.sort(key=lambda x: x[index], reverse=True)

        if len(filteredRows) != 0:
            return self.__class__(rows=filteredRows, headers=self.columnHeaders())
        else:
            return None

    def _createColumnHash(self, columnHeader):

        '''Creates a general hash from a given matrix column'''

        hashColumnIndex = self.indexOfColumnWithHeader(columnHeader)
        self.columnHash = {}
        index = 0
        for row in self.matrix:
            key = row[hashColumnIndex]
            if key not in self.columnHash:
                self.columnHash[key] = []

            self.columnHash[key].append(index)
            index += 1

    def keys(self):

        '''Returns the current hash keys'''

        return list(self.columnHash.keys())

    def setKeyColumn(self, columnHeader):

        '''Sets the hash column to \e columnHeader

        Raises a ValueError if no column has the supplied header'''

        self.currentKeyColumn = columnHeader
        self._createColumnHash(columnHeader)

    def keyColumn(self):

        '''Returns the header of the current key column or None if none has been set'''

        return self.currentKeyColumn

    def rowIndexesForKey(self, key):

        '''Returns the indexes of the rows containing key in the current hash column.

        Raises KeyError if no column contains key
        Returns None if no key column has been set'''

        if self.columnHash is not None:
            return tuple(self.columnHash[key])
        else:
            return None

    def rowsForKey(self, key):

        '''Returns the rows corresponding to the given key

        Raises KeyError if no column contains key
        Returns None if no key column has been set'''

        if self.columnHash is not None:
            indexes = self.columnHash[key]
            return [self.row(index) for index in indexes]
        else:
            return None

    def matrixForKey(self, key, name=None):

        '''Returns a matrix containing the rows matching key'''

        if name == None:
            name = "%s-%s" % (self.name(), key)

        return self.__class__(rows=self.rowsForKey(key),
                      headers=self.columnHeaders(),
                      name=name)

    def splitByKeys(self):

        '''Splits a matrix into submatrices based on key-col.

        Each matrix will correspond to the rows matching key and its name will be the key.'''

        matrices = []
        for key in list(self.keys()):
            m = self.matrixForKey(key)
            m.setName("%s" % key)
            matrices.append(m)

        return matrices


    def sortColumn(self, columnHeader, idColumnHeader, descending=True):

        '''Sorts the column given by 'columnHeader'.

         If idColumnHeader is supplied this method returns a list of (idValue, value) pairs
         Otherwise it returns a list of values.
         The use of idValue is to easily identify which records the sorted values correspond to

        Params:
            columnHeader: The name of the column to sort
            idColumnHeader: The name of a column containing a string identifiying each row '''

        data = self.column(columnHeader)
        if idColumnHeader is not None:
            data = list(zip(self.__getattr__(idColumnHeader), data))
            data.sort(key=lambda x: x[1], reverse=descending)
        else:
            data = list(data)
            data.sort(reverse=descending)

        return data


    def createHistogram(self, column, binSize, minLimit=None, maxLimit=None, verbose=False, normaliseMean=False):

        '''Creates a histogram from column

        Returns:
            A Matrix instance containing the histogram'''

        import math

        data = self.sortColumn(column, idColumnHeader=None, descending=False)

        if normaliseMean:
            mean = reduce(operator.add, data)/len(data)
            print("Normalising using mean %lf" % mean, file=sys.stderr)
            data = [el/mean for el in data]

            if minLimit is not None:
                minLimit = minLimit/mean
                print("Min limit scaled to %lf" % minLimit, file=sys.stderr)

            if maxLimit is not None:
                maxLimit = maxLimit/mean
                print("Max limit scaled to %lf" % maxLimit, file=sys.stderr)

            if binSize is not None:
                binSize = binSize/mean
                print("Bin-size scaled to %lf" % binSize, file=sys.stderr)

        min = float(data[0]) - binSize
        max = float(data[-1]) + binSize

        if minLimit is not None:
            print("Over-riding %lf with %lf" % (min, minLimit), file=sys.stderr)
            min = minLimit

        if maxLimit is not None:
            print("Over-riding %lf with %lf" % (max, maxLimit), file=sys.stderr)
            max = maxLimit

        if verbose:
            print("Minimum %lf. Maximum %lf" % (min, max), file=sys.stderr)

        histRange = max - min

        #The number of bins is the ceil(range/binSize)
        #This means the last bin covers a region beyond the max range if this fraction is not an integer.

        #If histRange/binSize is an integer there will be an issue if
        #MaxVal == Upper bound of the last bin e.g. range is [0,1] and MaxVal is 1.0
        #This is because here bin ranges are inclusive of lower bound and exclusive of upper
        #i.e. values are assigned as (floor((element-min)/binSize))
        #In this case the MaxVal values will not be assigned to a bin unless something is done
        #Instead they will cause an IndexError (as there bin index is one more than the total bins)
        #Here this is workaround by assigning any such values to the last bin.
        #Hence the last bins range is inclusive of upper and lower bounds unlike the others

        nbins = histRange/binSize
        bins = int(math.ceil(nbins))

        if verbose:
            print("Range %lf. Bins %lf" % (histRange, bins), file=sys.stderr)

        matrix = []
        minOutliers = 0
        maxOutliers = 0

        for i in range(bins):
            row = [min + (i + 0.5)*binSize, 0, 0, 0]
            matrix.append(row)

        upperCount=0
        for element in data:
            #Handle elements falling exactly on last bin upper bound
            if element == max:
                index=bins-1
                upperCount+=1
            else:
                index = int(math.floor((element - min)/binSize))
                #print element, index, matrix[index][0]
                if index < 0:
                    print("Min outlier ", element, file=sys.stderr)
                    minOutliers = minOutliers + 1
                    continue
                elif index >= bins:
                    print("Max outlier ", element, file=sys.stderr)
                    maxOutliers = maxOutliers + 1
                    continue

            matrix[index][1] = matrix[index][1] + 1

        for index in range(len(matrix)):
            if index > 0:
                matrix[index][2] = matrix[index-1][2] + matrix[index][1]
            else:
                matrix[index][2] = matrix[index][1]

            matrix[index][3] = matrix[index][1]/len(data)


        print("%d points. %d min outliers %d max outliers" % (len(data), minOutliers, maxOutliers), file=sys.stderr)
        if upperCount != 0:
            print('%d values falling on upper bound of last bin (%lf) were included in last bin' % (upperCount, max), file=sys.stderr)

        m = Matrix(rows=matrix, headers=[column, 'count', 'cumulative', 'prob'], name='Histogram')
        total = m[-1][2]
        cdf = [el/total for el in m.cumulative]
        m.addColumn(cdf, header='cdf')

        return m.trim(-1)

    def create2DHistogram(self, cols, binSize, minLimits, maxLimits, verbose=False):

        import math

        #Internal function for getting limits
        def GetLimits(column, minLimit, maxLimit):

            data = self.sortColumn(column, idColumnHeader=None, descending=False)

            min = float(data[0])
            max = float(data[-1])

            if minLimit is not None:
                print("Over-riding %lf with %lf" % (min, minLimit), file=sys.stderr)
                min = minLimit

            if maxLimit is not None:
                print("Over-riding %lf with %lf" % (max, maxLimit), file=sys.stderr)
                max = maxLimit

            if verbose:
                print("Minimum %lf. Maximum %lf" % (min, max), file=sys.stderr)

            return min, max

        #Internal function for assigning bin indexes
        def AssignBin(element, min, max, bins, binSize, minOutliers, maxOutliers, upperCount):

            if element == max:
                index=bins-1
                upperCount+=1
            else:
                index = int(math.floor(element - min)/binSize)
                if index < 0:
                    print("Min outlier ", element, file=sys.stderr)
                    minOutliers = minOutliers + 1
                    raise ValueError
                elif index >= bins:
                    print("Max outlier ", element, file=sys.stderr)
                    maxOutliers = maxOutliers + 1
                    raise ValueError

            return index


        min = []
        max = []
        histRange = []
        nbins = []
        bins = []

        for i in range(2):
            print('Getting limits of column %s' % cols[i], file=sys.stderr)
            a, b = GetLimits(cols[i], minLimits[i], maxLimits[i])
            min.append(a)
            max.append(b)
            histRange.append(max[i] - min[i])
            nbins.append(histRange[i]/binSize[i])
            bins.append(int(math.ceil(nbins[i])))

            if verbose:
                print("Dim %d. Range %lf. Bins %lf" % (i, histRange[i], bins[i]), file=sys.stderr)

        matrix = []

        #Create all bin entries
        for i in range(bins[0]):
            xIndex = min[0] + (i + 0.5)*binSize[0]
            for j in range(bins[1]):
                row = [xIndex, min[1] + (j + 0.5)*binSize[1], 0]
                matrix.append(row)

        data = list(zip(self.columnWithHeader(cols[0]),self.columnWithHeader(cols[1])))

        indexes = [0,0]
        minOutliers = [0,0]
        maxOutliers = [0,0]
        upperCount= [0,0]
        for element in data:
            try:
                for i in range(2):
                    indexes[i] = AssignBin(element[i], min[i], max[i], bins[i], binSize[i], minOutliers[i], maxOutliers[i], upperCount[i])
                index = indexes[1] + indexes[0]*bins[1]
                matrix[index][2] += 1
            except ValueError:
                pass


        print("%d points" % (len(data)), file=sys.stderr)
        print("Dim 1. %d min outliers %d max outliers" % (minOutliers[0], maxOutliers[0]), file=sys.stderr)
        print("Dim 2. %d min outliers %d max outliers" % (minOutliers[1], maxOutliers[1]), file=sys.stderr)

        print('Dim 1. %d values falling on upper bound (%lf) were included in last bins' % (upperCount[0], max[0]), file=sys.stderr)
        print('Dim 2. %d values falling on upper bound (%lf) were included in last bins' % (upperCount[1], max[1]), file=sys.stderr)

        return Matrix(rows=matrix, headers=[cols[0], cols[1], 'count'], name='Histogram')

    def trim(self, cdfCol):

        '''Removes redundant rows from histograms

        This are rows after the point where the cdf==1.0'''

        cdf = self.column(cdfCol)

        for i in range(len(cdf)):
            # There must be a cdf entry  == 1.0
            if abs(1.0 - cdf[i]) < 1E-10:
                break

        if i != self.numberRows - 1:
            self.matrix = self.matrix[0:i + 1]
            self.numberRows = i+1  # Update column hash if present
            if self.keyColumn() is not None:
                self._createColumnHash(self.keyColumn())

        return self

    def lower(self):

        '''Returns a transformation of the receiver where all strings are lower-case.

        Note: This method may no work correctly if columns have mixed types.
        If there are strings then the first entry must be a string for the conversion to work'''

        headers = [h.lower() for h in self.columnHeaders()]

        m = self.__class__()

        for header in self.columnHeaders():
            c = self.column(header)
            if isinstance(c[0], str) or isinstance(c[0], newstr):
                c = [e.lower() for e in c]

            m.addColumn(c)

        m.setColumnHeaders(headers)

        return m

def OuterTranspose(matrices, column, labels=None, labelName=None):

    '''Converts a set of matrices to a alternate represenation set

   A matrix is a labelled set (matrix index/name) of measurements (rows) of different variables (column-headers).
   Each row in a matrix is implicitly labelled by the matrix label.

   This method converts a variable into matrix label and the set of labels into a variable (column header)

   For example,the outer dimension (matrix label) is iteration and an inner dimension (column) is concentration.
   If iteration and concentration are transposed the output would be a set of matrix corresponds to a concentration
   each with a column giving various iteration values.

   In code;

   1. f = FlattenMatrices(matrices, matrixLabel="iteration")
   2. f.setKeyColumn("concentration")
   2. transpose = f.splitByKey()

   Parameters:
       matrices: A list of utilities.data.Matrix objects
       labels: A list of values, one for each element of matrices
       labelName: A identifier for the set of labels e.g. iteration, concentration
       column: A column header common to all the the elements of matrices.
           NB: All matrices must contain the full set of possible values for this column

   Returns
       newLabels: The set of possible values for the chosen column (h2)
       matrices: A array of matrices one for each of the new labels.
           The new matrices have the same columns as the old with the addition of one
           for labelName.
   '''

    if labels is None:
        labels = list(range(len(matrices)))

    if labelName is None:
        labelName = 'index'

    matrices[0].setKeyColumn(column)
    newLabels = list(matrices[0].keys())

    newMatrices = []
    for l in newLabels:
        rows = []
        for label, m in zip(labels, matrices):
            m.setKeyColumn(column)
            rs = m.rowsForKey(l)
            rs = [[label] + list(row) for row in rs]
            rows.extend(rs)

        m = Matrix(rows=rows,
                  headers=[labelName] + list(matrices[0].columnHeaders()))
        m.setName('%s %s' % (column, str(l)))
        newMatrices.append(m)

    return newLabels, newMatrices

def FlattenMatrices(matrices, labels=None, labelName="index"):

    '''
    Converts a list of matrices into a single 2-D matrix.

    i.e. flattens a 3-D data-frame/matrix to 2-D

    The 2-D matrix has an additional column "labelName" whose value for each row is the
    label of the matrix the row came from.

    Args:
        matrices: A set of utilities.data.Matrix instances
        labels: A set of labels for the matrices (floats/string/ints). Defaults to their index in matrices
        labelName: A name for the labels. Default to index

    Returns:
        A matrix.

    '''

    if labels is None:
        labels = list(range(len(matrices)))

    rows = []
    for (l, m) in zip(labels, matrices):
        for row in m:
            rows.append([l] + list(row))

    matrix = Matrix(rows=rows,
                   headers=[labelName] + list(matrices[0].columnHeaders()))

    return matrix
