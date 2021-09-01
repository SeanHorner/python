import os
from os import scandir
import glob
import fnmatch
from pathlib import Path
from datetime import datetime
import tempfile
from tempfile import TemporaryFile

# This python file covers many aspects of file io operations, as such it probably shouldn't be run and should only be
# used for reference on the many file io methods and their usage.

# Python's "with open(...) as ..." Pattern
# The most common paradigm of file manipulation, called the context manager protocol, is stated as follows:

with open('data.txt', 'r') as f:
    data = f.read()

# Effectively this code is opening the file at the path provided, arg_1, in the mode specified, arg_2 and assigns it
# the handle, i.e. shortcut name, of 'f'. The available modes are 'r' for 'read', 'w' for 'write', and 'rw' for
# 'read and write'. The bytes or binary flag can also be used with any of the three -- 'rb', 'wb' ,'rwb' -- to read
# data from or write data to the file serialized, i.e. in binary. Then the code reads out the contents of the file to
# the variable $data and then, once the code block is complete, it automatically closes the file.
# An example of creating a file to store output is as follows:

data_2 = 'some data to be written to the file'
with open('data.txt', 'w') as f:
    f.write(data_2)

# Using the os module built into Python, its possible to simplify tasks such as listing the files and directories at a
# given path using the os.listdir() method:

# NOTE: Since all imports are done at the top of the file, in trying to keep this script runnable as well as educational
# imports that are necessary for code blocks will be commented out in that block, like so:

# import os

entries = os.listdir('test_directory/')
#
#   ['sub_dir_c', 'file1.py', 'sub_dir_b', 'file3.txt', 'file2.csv', 'sub_dir']

# That output is difficult to read, so instead the list can be looped through and each item printed out in order
for entry in entries:
    print(entry)
#
#   sub_dir_c
#   file1.py
#   sub_dir_b
#   file3.txt
#   file2.csv
#   sub_dir

# In modern Python (>3.5) an excellent alternative to os.listdir() is os.scandir() which returns an iterator instead of
# a list. For example, if it is printed directly:

# import os

entries_2 = os.scandir('test_directory/')
print(entries_2)
#
#   <posix.ScandirIterator object at 0x7f5b047f3690>

# Iterating over the iterator, the entries can be printed out sequentially as before:

for entry in entries_2:
    print(entry.name)

# Note the use of the name attribute, since the ScandirIterator is returning/pointing to an object

# A second modern way (>3.4) to get all the files in a directory is to use the pathlib module, e.g.

# from pathlib import Path

entries_3 = Path('test_directory/')
for entry in entries_3.iterdir():
    print(entry.name)
#
#   sub_dir_c
#   file1.py
#   sub_dir_b
#   file3.txt
#   file2.csv
#   sub_dir

# pathlib.Path() objects have an .iterdir() method that creates an iterator pointing to all files in the given
# directory. By traversing each entry in that iterator file names and attributes can be printed/manipulated. The
# pathlib module provides many helpful abstractions to treat paths in a straight-forward, object-oriented paradigm.
# pathlib is considered to be better form than using os calls, since it requires fewer imports and has greater code
# flexibility, but all three methods are valid ways of garnering the information.

# Notice that all of these methods provide both files and subdirectories. To be a little more specific, say to gather
# only file names of files in the specified directory, some modifications/filters can be used. For example, using
# the first method, os.listdir():

# import os

basepath = 'test_directory/'
for entry in os.listdir(basepath):
    if os.path.isfile(os.path.join(basepath, entry)):
        print(entry)

#
#   file1.py
#   file3.txt
#   file2.csv

# Here the os.listdir() returns a list of file names in the basepath directory and for each entry on the list, i.e.
# for each file and directory name, the check os.path.isfile() is done on the combined path of the basepath plus the
# entry name (e.g. basepath + entry[0] = 'test_directory/file1.py'). If the entry corresponds to a file, i.e. passes the
# .isfile() check, then it is printed; if not, not.

# Here is the same implementation using os.scandir():

# import os

# basepath = 'test_directory/'
with os.scandir(basepath) as entries_4:
    for entry in entries_4:
        if entry.is_file():
            print(entry.name)
#   file1.py
#   file3.txt
#   file2.csv

# And finally, here's the third method, pathlib.Path()
# from pathlib import Path

basepath = Path('test_directory/')
files_in_basepath = basepath.iterdir()
for item in files_in_basepath:
    if item.is_file():
        print(item.name)
#
#   file1.py
#   file3.txt
#   file2.csv

# Obviously this method is nice, simple, and again, no os import messiness.
# It could be written even more Pythonic by combining the for loop and the if statement to make a generator:

# from pathlib import Path
# basepath = Path('test_directory/')

files_in_basepath_2 = (entry for entry in basepath.iterdir() if entry.is_file())
for item in files_in_basepath_2:
    print(item)
#
#   file1.py
#   file3.txt
#   file2.csv

# The same functionality can be used to find specifically subdirectories either by negating the if statements that
# check is_file() or by passing the is_dir() method. An example of this using the pathlib method is below:

# from pathlib import Path

# basepath = Path('test_directory/')
for entry in basepath.iterdir():
    if entry.is_dir():
        print(entry.name)
#
#   sub_dir_c
#   sub_dir_b
#   sub_dir


# Another important file io function is finding and displaying file attributes, such as creation time, last access,
# permissions, etc. The following code uses the os.scandir() method to check the last modification time of each item
# in the directory.

# import os

with os.scandir('test_directory/') as dir_contents:
    for entry in dir_contents:
        info = entry.stat()
        print(info.st_mtime)
#
#   ...
#   1539032199.0052035
#   1539032469.6324475
#   1538998552.2402923
#   1540233322.4009316
#   1537192240.0497339
#   1540266380.3434134

# In this code, the ScandirIter named dir_contents is created, the for loop loops through each entry in the iterator,
# and for each entry it uses the stat() method to copy the entry attributes to the variable info, which is then used to
# print the desired attribute, st_mtime.
# The pathlib method has a corresponding method for file attribute retrieval, e.g.

# from pathlib import Path

current_dir = Path('test_directory/')
for path in current_dir.iterdir():
    info = path.stat()
    print(info.st_mtime)
#
#   ...
#   1539032199.0052035
#   1539032469.6324475
#   1538998552.2402923
#   1540233322.4009316
#   1537192240.0497339
#   1540266380.3434134

# The above code could be modified to return a more human readable time value, say a datetime:

# from os import scandir
# from datetime import datetime


def convert_date(timestamp):
    d = datetime.utcfromtimestamp(timestamp)
    formatted_date = d.strftime('%d %b %Y')
    return formatted_date


def get_files():
    dir_entries = scandir('test_directory/')
    for entry_2 in dir_entries:
        if entry_2.is_file():
            info_2 = entry_2.stat()
            print(f'{entry_2.name}\t Last Modified: {convert_date(info_2.st_mtime)}')
#
#   file1.py        Last modified:  04 Oct 2018
#   file3.txt       Last modified:  17 Sep 2018
#   file2.txt       Last modified:  17 Sep 2018

# In addition to looking for files and directories, another very common file operation is creating directories,
# especially when programs need to store their data, for example a saved configuration file, or a game save.
# This operation can be performed either using the os or the pathlib module. In the os module there are two options:
#   1.  os.mkdir(path)      which can create a single subdirectory.
#   2.  os.makedirs(path)   which can create multiple directories (i.e. all intermediate directories necessary to create
#                           the path that is passed). For example, from /opt os.makedirs('local/needed/stuffs') would
#                           create the directory 'needed' as well as 'stuff'.
# The pathlib module contains one method that is useful for this operation:
#   1.  pathlib.Path.mkdir  which can create single or multiple subdirectories.
# The usage of these methods is as follows:

# import os


os.mkdir('example_directory/')

# from pathlib import Path

p = Path('example_directory_2/')
p.mkdir()

# NOTE: the second code uses a different path, that's because it would throw an unhandled FileExistsError otherwise.
# In fact both of these methods will throw a FileExistsError if a directory by that name already exists.
# Because of that it is best to use a try-catch block when doing directory operations like this:

# from pathlib import Path

p = Path('example_directory/')
try:
    p.mkdir()
except FileExistsError as e:
    print(e)

# With the pathlib option it is also possible to ignore this error directly from the method call by passing the method
# with the option exist_ok set to true, i.e.:

# from pathlib import Path

p = Path('example_directory/')
p.mkdir(exist_ok=True)

# os.makedirs() works in much the same way, except it can be passed a nest of directories and create all necessary
# intermediate directories. But it can also use other keywords to perform many useful file operations. For example, it
# can set the file permissions for directories when they are made by passing it the 'mode' keyword with a triple octal
# for the user/group/other access permissions for the file, e.g.

# import os

os.makedirs('2018/10/05', mode=0o770)
# Creates the following directory structure:
#   .
#   |
#   └── 2018/
#       └── 10/
#           └── 05/

# The tag 0o (zero-oh) in front of 770 tells the compiler that it is an octal number. All the directories created will
# have this values for its access permissions.

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# As a reminder, user/group/guest permissions and their octal representations work as follows:
#
#   File and directory access permissions can be changed with the 'chmod 0oNNN {file_name}' command which changes the
#   access permissions of the passed file or directory to the triple octal (NNN) in the following way:
#
#   1st number -> user permissions for the listed owner (change with 'chown user:group' command)
#   2nd number -> group permissions for the listed user's group (change with the same command)
#   3rd number -> guest permissions for anyone who isn't the user or the user's group
#
#   wrx  (<- 'write', 'read', 'execute')
#   001  =  1   : execute permission only
#   010  =  2   : write permission only
#   011  =  3   : write and execute permissions
#   100  =  4   : read permission only
#   101  =  5   : read and execute permissions
#   110  =  6   : read and write permissions
#   111  =  7   : read, write, and execute (full) permissions
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# The pathlib.Path.mkdir() method can create intermediate folder structures if it is passed with keyword 'parents'

# from pathlib import Path

p = Path('2018/10/05')
p.mkdir(parents=True, exist_ok=True)

# As can be seen, the exist_ok flag is also usable here. It also works with the os.makedirs() method.

# **File Name Pattern Matching** is a common occurrence when working with files. Knowing that an entry is a file is
# useful, knowing whether that file is a .py or .txt file can be critical. There are many methods that can be used to
# accomplish this, namely:
#   ~  Python's endswith() and startswith() string methods
#   ~  fnmatch.fnmatch()
#   ~  glob.glob()
#   ~  pathlib.Path.glob()
# For example,

# import os

for f_name in os.listdir('some_directory/'):
    if f_name.endswith('.txt'):
        print(f_name)
#
#   data_01.txt
#   data_03.txt
#   data_03_backup.txt
#   data_02_backup.txt
#   data_02.txt
#   data_01_backup.txt

# While string matching with Python's built-in string methods can be helpful, its not as functional as a purpose-built
# file name matching method like fnmatch(). An example of such increased functionality is the use of wild cards in the
# match search, making it possible to do some pretty intricate matching. A simple example might be:

# import os
# import fnmatch

for file_name in os.listdir('some_directory/'):
    if fnmatch.fnmatch(file_name, '*backup*'):
        print(file_name)
#
#   data_01_backup.txt
#   data_02_backup.txt
#   data_03_backup.txt

# The next method of filename matching is the glob.glob(). Used like so:

# import glob

glob.glob('*[0-9]*.txt')
#
#   data_01.txt
#   data_03.txt
#   data_03_backup.txt
#   data_02_backup.txt
#   data_02.txt
#   data_01_backup.txt

# This glob retrieves all filenames that have a digit in the filename and the .txt extension.
# The glob method also makes recursive searches by giving the keyword argument 'recursive'. It's also important to
# know that there is a second method in the module called iglob() which returns an iterator instead of a list, ergo
# the print loop in this code:

# import glob

for file in glob.iglob('**/*.py', recursive=True):
    print(file)
#
#   admin.py
#   tests.py
#   sub_dir/file1.py
#   sub_dir/file2.py

# The final method that can be used is the pathlib module's Path.glob() method. It functions essentially identically
# to the glob.iglob() method above:

# from pathlib import Path

p = Path('walk_test/')
for name in p.glob('*.p*'):
    print(name)
#
#   admin.py
#   tests.py
#   docs.pdf    Note: not actually there, but if it were in the directory it would get scooped up too.

# The next segment covers directory tree walking, i.e. moving through a directory and accurately capturing the file
# structure. The first method that can be used is the os.walk() method which returns three values on each iteration:
# The current directory name (dirpath below), a list of folders in the current directory (dirnames below), and a list
# of files in the current directory (files below). It functions like so:
for dirpath, dirnames, files in os.walk('walk_test/'):
    print(f'Found directory: {dirpath}')
    for file_name in files:
        print("\t" + file_name)
#
#   Found directory: walk_test
#       test1.txt
#       test2.txt
#   Found directory: walk_test/folder_1
#       file1.py
#       file3.py
#       file2.py
#   Found directory: walk_test/folder_2
#       file4.py
#       file5.py
#       file6.py

# The loop starts by checking the top directory for folders and files, then it prints the files in the top directory.
# On the next pass it checks the first (lexicographically) sub-directory for folders and files, then prints out the
# files, etc.

# Often programs make use of temporary file space to hold data while processing or for holding session setting or any
# number of other useful use cases. In regards to this, Python offers an excellent module called, inventively, tempfile.
# The beauty of using this module is that Python will auto-clean these temp files once they are closed.
# An example is as follows:

# from tempfile import TemporaryFile

# Create a temporary file and write some data to it
fp = TemporaryFile('w+t')
fp.write('Hello universe!')

# Go back to the beginning and read the data from the file
fp.seek(0)
data = fp.read()

# Close the file, after which it will be removed
fp.close()

print(data)
#
#   Hello universe!

# In the above example, the temp file was opened with the mode 'w+t', for 'write' and 'text file' .Note, there is no
# file name argument since it doesn't have a path. TemporaryFile() is also a context manager, thus it can be used in
# the "with {} as {}" protocol, e.g. :

with TemporaryFile('w+t') as fp:
    fp.write('Hello universe!')
    fp.seek(0)
    data = fp.read()
    print(data)
# File is now closed and removed
#
#   Hello universe!

# If a file has been created
