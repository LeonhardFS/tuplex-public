#!/usr/bin/env python3
# (c) 2017 - 2024
# creates the zip file to deploy to Lambda, adapted from https://github.com/awslabs/aws-lambda-cpp/blob/9df704157539388b091ff0936f79c34d4ca6993d/packaging/packager
# python script is easier to read though/adapt

import os
import sys
import zipfile
import subprocess
import tempfile
import logging
import shutil
import re
import glob
import stat
import argparse
import time
from sys import platform

try:
    from tqdm import tqdm
except:
    def tqdm(gen):
        return gen

# 5MB threshold for UPX compression.
UPX_THRESHOLD = 5 * 1000 * 1000

def cmd_exists(cmd):
    """
    checks whether command `cmd` exists or not
    Args:
        cmd: executable or script to check for existence

    Returns: True if it exists else False

    """

    #TODO: better use type pacman > /dev/null 2>&1?
    return shutil.which(cmd) is not None

def get_list_result_from_cmd(cmd, timeout=2):
    p = subprocess.Popen(cmd, stdin=None, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate(timeout=timeout)

    if stderr is not None and len(stderr) > 0:
        logging.error("FAILURE")
        logging.error(stderr)
        return []

    if stdout is None or 0 == len(stdout):
        return []

    return stdout.decode().split('\n')

def query_libc_shared_objects(NO_LIBC):
    # use pacman, dpkg, rpm to query libc files...
    libc_files = []

    # for each command check whether it exists
    pacman_files = get_list_result_from_cmd(['pacman', '--files', '--list', '--quiet', 'glibc']) if cmd_exists('pacman') else []
    dpkg_files = get_list_result_from_cmd(['dpkg-query', '--listfiles', 'libc6']) if cmd_exists(
        'dpkg-query') else []
    rpm_files = get_list_result_from_cmd(['rpm', '--query', '--list', 'glibc']) if cmd_exists(
        'rpm') else []

    # filter so only shared objects are contained...
    libc_files = pacman_files + dpkg_files + rpm_files
    libc_files = list(filter(lambda path: re.search(r"\.so$|\.so\.[0-9]+$", path), libc_files))

    if not NO_LIBC:
        assert len(libc_files) > 0, 'Could not retrieve any LIBC files. Broken?'

    return libc_files

# from https://stackoverflow.com/questions/1094841/get-a-human-readable-version-of-a-file-size
def human_size(bytes, units=[' bytes','KB','MB','GB','TB', 'PB', 'EB']):
    """ Returns a human readable string representation of bytes """
    return str(bytes) + units[0] if bytes < 1024 else human_size(bytes>>10, units[1:])

def get_uncompressed_size(zip_file_path):
    # unzip -l /build/tplxlam.zip | tail -1 | cut -d ' ' -f1
    output = subprocess.getoutput(f"unzip -l {zip_file_path} | tail -1 | cut -d ' ' -f1")
    return int(output.strip())

UPX_PATH="upx"

def check_or_download_upx():
    global UPX_PATH

    UPX_DOWNLOAD_URL="https://github.com/upx/upx/releases/download/v4.2.4/upx-4.2.4-amd64_linux.tar.xz"

    # Check whether upx is installed or not, if not download (small enough)
    try:
        subprocess.check_output(UPX_PATH)
    except FileNotFoundError:
        logging.info(f"UPX not found, downloading from {UPX_DOWNLOAD_URL}.")
        if not sys.platform.startswith("linux"):
            raise Exception(f"Can download UPX only for linux, platform is {sys.platform}")
        import tempfile
        tmp = tempfile.NamedTemporaryFile(delete=False)

        import urllib.request
        import os

        # Adding user_agent information
        opener=urllib.request.build_opener()
        opener.addheaders=[('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]
        urllib.request.install_opener(opener)

        # Get resource
        urllib.request.urlretrieve(UPX_DOWNLOAD_URL, tmp.name)
        logging.info(f"Got upx ({human_size(os.path.getsize(tmp.name))}) and stored to {tmp.name}.")

        tmp_dir = tempfile.mkdtemp() # tempfile.TemporaryDirectory() # no cleanup

        output = subprocess.getoutput(f"tar xf {tmp.name} -C {tmp_dir}")

        UPX_PATH = os.path.join(tmp_dir, "upx-4.2.4-amd64_linux", "upx")
        logging.info(f"UPX located in {UPX_PATH}")

# UPX uses by default 8 as compression level.
# That's good, but slow to unpack.
# cf. https://github.com/upx/upx/blob/devel/doc/upx-doc.txt
def zip_with_upx(zip, src, dest, upx_compression_level=3):
    import mimetypes

    assert os.path.exists(src)

    assert 'best' == upx_compression_level or (1 <= upx_compression_level <= 9 and isinstance(upx_compression_level, int))

    # Check if ELF file, else regularly encode.
    max_read_size = 16
    with open(src, 'rb') as fd:
        file_head = fd.read(max_read_size)

    is_elf_file = file_head.startswith(bytes([0x7f, 0x45, 0x4c, 0x46]))

    if os.path.getsize(src) >= UPX_THRESHOLD and is_elf_file:
        logging.info(f"File {src} is ELF and larger than threshold, compressing with upx.")

        tmp_dir = tempfile.mkdtemp()
        tmp_name = os.path.join(tmp_dir, "compressed.bin")
        out = subprocess.getoutput(f"{UPX_PATH} -{upx_compression_level} -o {tmp_name} {src}")
        print(out)

        if os.path.getsize(tmp_name) == 0:
            raise FileNotFoundError("compressed file not existing.")

        logging.info(f"UPX compressed {os.path.basename(src)} from {human_size(os.path.getsize(src))} to {human_size(os.path.getsize(tmp_name))}, adding to ZIP.")
        zip.write(tmp_name, dest)
        shutil.rmtree(tmp_dir)
    else:
        # regularly compress.
        zip.write(src, dest)

def main():
    # set logging level here
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)

    parser = argparse.ArgumentParser(description='Lambda zip packager')
    parser.add_argument('-o', '--output', type=str, dest='OUTPUT_FILE_NAME', default='tplxlam.zip',
                        help='output path where to write zip file')
    parser.add_argument('-i', '--input', type=str, dest='TPLXLAM_BINARY', default=os.path.join('dist/bin', 'tplxlam'),
                        help='input path of tplx binary')
    parser.add_argument('-r', '--runtime', dest='TPLX_RUNTIME_LIBRARY', type=str, default=os.path.join('dist/bin', 'tuplex_runtime.so'),
                        help="whether to resolve exceptions in order")
    parser.add_argument('-p', '--python', dest='PYTHON3_EXECUTABLE', type=str,
                        default='/opt/lambda-python/bin/python3.8',
                        help='path to python executable from which to package stdlib.')
    parser.add_argument('--no-libc', dest='NO_LIBC', action="store_true",
                        help="whether to skip packaging libc files or not.")
    parser.add_argument('--with-upx', help="Enable compression of shared objects/binary files with upx.", dest="with_upx", action="store_true")
    args = parser.parse_args()

    if args.with_upx:
        check_or_download_upx()


    OUTPUT_FILE_NAME=args.OUTPUT_FILE_NAME
    TPLXLAM_BINARY=args.TPLXLAM_BINARY
    TPLX_RUNTIME_LIBRARY=args.TPLX_RUNTIME_LIBRARY
    ## why is python3 needed?
    PYTHON3_EXECUTABLE=args.PYTHON3_EXECUTABLE
    NO_LIBC=args.NO_LIBC
    INCLUDE_LIBC=NO_LIBC is False

    if INCLUDE_LIBC:
        logging.info('Including libc files in zip.')



    pkg_loader = 'ld-linux-x86-64.so.2' # change to whatever is in dependencies...

    # bootstrap scripts

    binary_basename=os.path.basename(TPLXLAM_BINARY)

    # use this script here when libc is included => requires package loader
    bootstrap_script=f"""#!/bin/bash
set -euo pipefail
export AWS_EXECUTION_ENV=lambda-cpp
PKG_BIN_FILENAME={binary_basename}
exec $LAMBDA_TASK_ROOT/lib/{pkg_loader} --library-path $LAMBDA_TASK_ROOT/lib $LAMBDA_TASK_ROOT/bin/$PKG_BIN_FILENAME ${{_HANDLER}}
"""

    # use this script when libc is not included
    bootstrap_script_nolibc=f"""#!/bin/bash
set -euo pipefail
PKG_BIN_FILENAME={binary_basename}
export AWS_EXECUTION_ENV=lambda-cpp
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LAMBDA_TASK_ROOT/lib
exec $LAMBDA_TASK_ROOT/bin/$PKG_BIN_FILENAME ${{_HANDLER}}
"""

    # find python files
    logging.info('Python3 executable: {}'.format(PYTHON3_EXECUTABLE))
    py_stdlib_path = get_list_result_from_cmd([PYTHON3_EXECUTABLE, '-c', 'import sysconfig; print(sysconfig.get_path(\'stdlib\'))'])[0]
    py_site_packages_path = get_list_result_from_cmd([PYTHON3_EXECUTABLE, '-c', 'import sysconfig; print(sysconfig.get_path(\'purelib\'))'])[0]
    py_version = get_list_result_from_cmd([PYTHON3_EXECUTABLE, '-c', 'import sys; print(\'{}.{}\'.format(sys.version_info.major,sys.version_info.minor))'])[0]
    logging.info('Found Python standard lib in {}'.format(py_stdlib_path))
    logging.info('Found Python packages in {}'.format(py_site_packages_path))
    logging.info('Version of Python to package is {}'.format(py_version))

    # find all libc dependencies
    libc_libs = []
    if not NO_LIBC:
        libc_libs = query_libc_shared_objects(NO_LIBC)
        logging.info('Found {} files comprising LIBC'.format(len(libc_libs)))
    else:
        logging.info('NO_LIBC passed, make sure to have built everything on Amazon Linux 2 machine.')

    # use file with ld- as loader!

    # find dependencies using ldd
    # -> for both binary AND runtime

    ldd_dependencies = get_list_result_from_cmd(['ldd', TPLXLAM_BINARY])
    ldd_dependencies = list(map(lambda line: line.strip(), ldd_dependencies))

    # for each line, extract name, original_path
    def extract_from_ldd(line):
        if '=>' not in line:
            return '', ''

        parts = line.split('=>')
        head = parts[0]
        tail = parts[-1]
        name = head.strip()
        path = tail[:tail.find('(')].strip()

        return name, path

    # get pkg_loader name
    for line in ldd_dependencies:
        line = line.strip()
        if line == '':
            continue
        head = line.split()[0]
        if os.path.basename(head).startswith('ld-'):
            pkg_loader = os.path.basename(head)

    logging.info('Found package loader {}'.format(pkg_loader))

    # exclude where no files are (i.e. linux-vdso)
    ldd_dependencies = list(filter(lambda t: t[1] != '', map(extract_from_ldd, ldd_dependencies)))

    logging.info('Found {} dependencies'.format(len(ldd_dependencies)))
    #
    # # find pkg loader
    # for path in libc_libs:
    #     filename = os.path.basename(path)
    #     if filename.startswith('ld-'):
    #         logging.info('Found package loader {}'.format(filename))
    #         pkg_loader = filename

    #compression=zipfile.ZIP_LZMA # use this for final file, because smaller!
    compression = zipfile.ZIP_DEFLATED # this is the only one that works for MacOS and on Lambda :/

    def create_zip_link(zip, link_source, link_target):
        zipInfo = zipfile.ZipInfo(link_source)
        zipInfo.create_system = 3  # System which created ZIP archive, 3 = Unix; 0 = Windows
        unix_st_mode = stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH
        zipInfo.external_attr = unix_st_mode << 16  # The Python zipfile module accepts the 16-bit "Mode" field (that stores st_mode field from struct stat, containing user/group/other permissions, setuid/setgid and symlink info, etc) of the ASi extra block for Unix as bits 16-31 of the external_attr
        zip.writestr(zipInfo, link_target)

    with zipfile.ZipFile(OUTPUT_FILE_NAME, 'w', compression=compression, compresslevel=9) as zip:
        logging.info('Writing bootstrap script {}'.format('NO_LIBC=True' if NO_LIBC else ''))

        # Mark bootstrap script as executable
        # https://stackoverflow.com/questions/434641/how-do-i-set-permissions-attributes-on-a-file-in-a-zip-file-using-pythons-zip

        bootstrap_info = zipfile.ZipInfo('bootstrap')
        bootstrap_info.date_time = time.localtime()
        # use 755 permissions and set for regular file
        bootstrap_info.external_attr = 0o100755 << 16

        if INCLUDE_LIBC and not args.with_upx:
            logging.info("Using bootstrap script with shipped libc preloader.")
            zip.writestr(bootstrap_info, bootstrap_script)
        else:
            logging.info("Basic bootstrap script, using environment libc.")
            zip.writestr(bootstrap_info, bootstrap_script_nolibc)

        # adding actual execution scripts
        logging.info('Writing C++ binary...')

        # Use UPX? If so, create temporary file to compress to
        if args.with_upx:
            zip_with_upx(zip, TPLXLAM_BINARY, 'bin/' + os.path.basename(TPLXLAM_BINARY))
        else:
            zip.write(TPLXLAM_BINARY, 'bin/' + os.path.basename(TPLXLAM_BINARY))
        logging.info('Writing Tuplex runtime...')
        if args.with_upx:
            zip_with_upx(zip, TPLX_RUNTIME_LIBRARY, 'lib/tuplex_runtime.so')
        else:
            zip.write(TPLX_RUNTIME_LIBRARY, 'lib/tuplex_runtime.so')

        # copy libc
        if INCLUDE_LIBC:
            logging.info('Writing libc files')
            for path in libc_libs:
                try:
                    # # for links, just write linked version to decrease size...
                    # # if that fails, simply only go for the else branch...
                    # if os.path.islink(path):
                    #     # cf. https://stackoverflow.com/questions/35782941/archiving-symlinks-with-python-zipfile on optimization
                    #     link_source = path
                    #     link_target = os.readlink(path)
                    #     logging.debug('Found Link: {} -> {}, writing link to archive...'.format(link_source, link_target))
                    #     create_zip_link(zip, link_source, link_target)
                    # else:
                    #     zip.write(path, os.path.join('lib/', os.path.basename(path)))
                    if args.with_upx:
                        if not os.path.exists(path):
                            logging.error(f"Path {path} not found, invoking default zip command.")
                            zip.write(path, os.path.join('lib/', os.path.basename(path)))
                        else:
                            zip_with_upx(zip, path, os.path.join('lib/', os.path.basename(path)))
                    else:
                        zip.write(path, os.path.join('lib/', os.path.basename(path)))

                except FileNotFoundError as e:
                    logging.warning('Could not find libc file {}, details: {}'.format(os.path.basename(path), e))

        logging.info('writing dependencies...')
        # write dependencies, skip whatever is in libc

        libc_libnames = set(map(lambda path: os.path.basename(path), libc_libs))

        for name, path in set(ldd_dependencies):
            if name in libc_libnames:
                continue

            # if os.path.islink(path):
            #     # cf. https://stackoverflow.com/questions/35782941/archiving-symlinks-with-python-zipfile on optimization
            #     link_source = path
            #     link_target = os.readlink(path)
            #     logging.debug('Found Link: {} -> {}, writing link to archive...'.format(link_source, link_target))
            #     create_zip_link(zip, link_source, link_target)
            # else:
            #     zip.write(path, os.path.join('lib', name))

            if args.with_upx:
                zip_with_upx(zip, path, os.path.join('lib', name))
            else:
                zip.write(path, os.path.join('lib', name))


        # now copy in Python lib from specified python executable!
        # TODO: compile them to pyc files, this should lead to smaller size...

        logging.info('Writing Python stdlib from {}'.format(py_stdlib_path))
        root_dir = py_stdlib_path

        paths = list(filter(os.path.isfile, glob.iglob(root_dir + '**/**', recursive=True)))

        # exclude numpy files...
        paths = list(filter(lambda path: 'numpy' not in path, paths))

        # TODO: exclude more files here to make this smaller and still keep it executable!!!

        logging.info('Found {} files in python stdlib to ship'.format(len(paths)))
        # for path in glob.iglob(root_dir + '**/**', recursive=True):
        #     if not os.path.isfile(path):
        #         continue

        py_arch_root = os.path.join('lib', 'python{}'.format(py_version))
        logging.info('Writing Python stdlib to path {} in archive'.format(py_arch_root))

        if not root_dir.endswith('/'):
            root_dir += '/'

        # There are a couple large files in the stdlib that should get excluded...
        # -> e.g. libpython3.8.a is 59.1MB
        # -> also the pip whl is 15.4MB
        # # get file sizes, list top5 largest files...
        # file_infos = list(map(lambda path: (path, os.stat(path).st_size), paths))
        # file_infos = sorted(file_infos, key=lambda t: -t[1])
        # file_infos = list(map(lambda t: (t[0], t[1])))
        # print(file_infos[:5])

        def exclude_from_packaging(path):

            # There's a folder config-3.11-x86_64-linux-gnu, which holds a lot of large/unnecessary files.
            # Exclude them from zipping to reduce file size.
            #      2052  10-12-2023 06:24   lib/python3.11/config-3.11-x86_64-linux-gnu/python-config.py
            #     11182  10-12-2023 06:24   lib/python3.11/config-3.11-x86_64-linux-gnu/Setup
            #      3418  10-12-2023 06:24   lib/python3.11/config-3.11-x86_64-linux-gnu/config.c
            #      1752  10-12-2023 06:24   lib/python3.11/config-3.11-x86_64-linux-gnu/config.c.in
            #      9262  10-12-2023 06:24   lib/python3.11/config-3.11-x86_64-linux-gnu/makesetup
            #    116448  10-12-2023 06:24   lib/python3.11/config-3.11-x86_64-linux-gnu/Makefile
            #       878  10-12-2023 06:24   lib/python3.11/config-3.11-x86_64-linux-gnu/Setup.bootstrap
            #     15358  10-12-2023 06:24   lib/python3.11/config-3.11-x86_64-linux-gnu/install-sh
            #      5377  10-12-2023 06:24   lib/python3.11/config-3.11-x86_64-linux-gnu/Setup.stdlib
            #     10736  10-12-2023 06:24   lib/python3.11/config-3.11-x86_64-linux-gnu/python.o
            #        41  10-12-2023 06:24   lib/python3.11/config-3.11-x86_64-linux-gnu/Setup.local
            # 111390872  10-12-2023 06:24   lib/python3.11/config-3.11-x86_64-linux-gnu/libpython3.11.a
            config_folder_regex = r".*\/config-\d+\.\d+-.*\/?"
            if re.match(config_folder_regex, path):
                path_size = os.stat(path).st_size
                logging.info(f"Excluding {path} ({human_size(path_size)}) from LAMBDA zip.")
                return False

            # exclude pyc cached files
            if '__pycache__' in path:
                return False

            # exclude test/ folder
            if 'test/' in path or 'tests/' in path:
                return False

            # exclude turtledemo
            if 'turtledemo/' in path:
                return False

            # keep.
            return True

        # exclude here certain paths
        num_before_exclusion = len(paths)
        paths = list(filter(exclude_from_packaging, paths))
        logging.info('Excluding {} files from runtime...'.format(num_before_exclusion - len(paths)))

        for path in tqdm(paths):
            # perform link optimization??
            # copy to lib/python<maj>.<min>
            target = os.path.join(py_arch_root, path.replace(root_dir, ''))
            logging.debug('{} -> {}'.format(path, target))

            # Use upx mode, because files could be large.
            if args.with_upx:
                zip_with_upx(zip, path, target)
            else:
                zip.write(path, target)

    if not os.path.isfile(OUTPUT_FILE_NAME):
        logging.error('Something went wrong, could not find file under {} ({})'.format(OUTPUT_FILE_NAME, os.path.realpath(OUTPUT_FILE_NAME)))
    else:
        logging.info('Done! Zipped Lambda stored in {}'.format(os.path.realpath(OUTPUT_FILE_NAME)))
        out_zip_path = os.path.realpath(OUTPUT_FILE_NAME)
        file_size = os.stat(out_zip_path).st_size
        file_size_mb = file_size / (1000.0 * 1000.0)
        uncompressed_file_size_mb = get_uncompressed_size(out_zip_path) / (1000.0 * 1000.0)
        logging.info(f"Zipped file size: {file_size_mb:.1f} MB")
        logging.info(f"Uncompressed file size (should be < 250 MB): {uncompressed_file_size_mb:.1f} MB")

if __name__ == '__main__':
    main()