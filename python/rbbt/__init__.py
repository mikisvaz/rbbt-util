import warnings
import sys
import os
import subprocess

def rbbt(cmd = None):
    if cmd is None:
        print("Rbbt")
    else:
        return subprocess.run('rbbt_exec.rb', input=cmd.encode('utf-8'), capture_output=True).stdout.decode()

def libdir():
    return rbbt('puts Rbbt.find(:lib)').rstrip()

def add_libdir():
    pythondir = os.path.join(libdir(), 'python')
    sys.path.insert(0, pythondir)

def path(subdir = None, base_dir = None):
    from pathlib import Path
    import os

    if (base_dir == 'base'):
        base_dir = os.path.join(Path.home(), ".rbbt")
    elif (base_dir == 'lib'):
        base_dir = libdir()
    else:
        for base_dir in ('lib', 'base'):
            file = path(subdir, base_dir)
            if os.path.exists(file):
                return file
        return path(subdir, 'base')

    if (subdir == None):
        return base_dir
    else:
        return os.path.join(base_dir, subdir)

def read(subdir, base_dir = None, encoding='utf-8'):
    file = path(subdir, base_dir)
    with open(file, encoding=encoding) as f:
        return f.read()

def inspect(obj):
    print(dir(obj))

def rich(obj):
    import rich
    rich.inspect(obj)

def log_tsv(tsv):
    print(tsv)
    print(tsv.keys())

def tsv_preamble(line, comment_char="#"):
    import re
    header = dict()
    entries = re.sub(f"^{comment_char}:", '', line)
    entries = re.sub(f"^{comment_char}:", '', line).split("#")
    for entry in entries:
        entry = entry.strip()
        key, value = entry.split("=")
        key = re.sub("^:","",key)
        value = re.sub("^:","",value)
        header[key] = value

    return header


def tsv_header(filename, sep="\t", comment_char="#", encoding='utf8'):
    import re

    f = open(filename, encoding=encoding)
    line = f.readline().strip()

    if (not line.startswith(comment_char)):
        header = {"fields":None, "type":"list", "start": 0}
    else:
        header = dict()
        start = 0
        if (line.startswith(f"{comment_char}:")):
            header["preamble"]=tsv_preamble(line, comment_char)
            if ("type" in header["preamble"]):
                header["type"] = header["preamble"]["type"]
            line = f.readline().strip()
            start = 1

        if (line.startswith(comment_char)):
            header["all_fields"] = re.sub(f"^{comment_char}", "", line).split(sep)
            header["key_field"] = header["all_fields"][0]
            header["fields"] = header["all_fields"][1:]

        header["start"] = start

    f.close()
    return header


def tsv_pandas(filename, sep="\t", comment_char="#", index_col=0, **kwargs):
    import pandas

    if (comment_char == ""):
        tsv = pandas.read_table(filename, sep=sep, index_col=index_col, **kwargs)
    else:
        header = tsv_header(filename, sep=sep, comment_char="#")

        if ("type" in header and header["type"] == "flat"):
            if ("sep" in header):
                sep=header["sep"]

            tsv = pandas.read_table(filename, sep=sep, index_col=index_col, header=None, skiprows=[0,1], **kwargs)

            if ("key_field" in header):
                tsv.index.name = header["key_field"]
        else:
            if ("sep" in header):
                sep=header["sep"]

            tsv = pandas.read_table(filename, sep=sep, index_col=index_col, header=header["start"], **kwargs)

            if ("fields" in header):
                tsv.columns = header["fields"]
                tsv.index.name = header["key_field"]

    return tsv

def tsv(*args, **kwargs):
    return tsv_pandas(*args, **kwargs)

def save_tsv(filename, df, key=None):
    if (key == None):
        key = df.index.name
    if (key == None):
        key = "Key"
    key = "#" + key
    df.to_csv(filename, sep="\t", index_label=key)


