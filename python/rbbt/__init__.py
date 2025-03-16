import sys
import os
import subprocess
import tempfile
import shutil
import pandas
import numpy


def cmd(cmd=None):
    if cmd is None:
        print("Rbbt")
    else:
        return subprocess.run('rbbt_exec.rb', input=cmd.encode('utf-8'), capture_output=True).stdout.decode()


def libdir():
    return cmd('puts Rbbt.find(:lib)').rstrip()


def add_libdir():
    pythondir = os.path.join(libdir(), 'python')
    sys.path.insert(0, pythondir)


def path(subdir=None, base_dir=None):
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

def benchmark():
    import time
    tic: float = time.perf_counter()
    try:
        yield
    finally:
        toc: float = time.perf_counter()
    print(f"Computation time = {1000*(toc - tic):.3f}ms")

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

def save_job_inputs(data):
    temp_dir = tempfile.mkdtemp()  # Create a temporary directory

    for name, value in data.items():
        file_path = os.path.join(temp_dir, name)

        if isinstance(value, str):
            file_path += ".txt"
            with open(file_path, "w") as f:
                f.write(value)

        elif isinstance(value, (bool)):
            with open(file_path, "w") as f:
                if value:
                    f.write('true')
                else:
                    f.write('false')

        elif isinstance(value, (int, float)):
            with open(file_path, "w") as f:
                f.write(str(value))

        elif isinstance(value, pandas.DataFrame):
            file_path += ".tsv"
            save_tsv(file_path, value)

        elif isinstance(value, numpy.ndarray) or isinstance(value, list):
            file_path += ".list"
            with open(file_path, "w") as f:
                f.write("\n".join(value))

        else:
            raise TypeError(f"Unsupported data type for argument '{name}': {type(value)}")

    return temp_dir


def run_job(workflow, task, name='Default', fork=False, clean=False, **kwargs):
    inputs_dir = save_job_inputs(kwargs)
    cmd = ['rbbt', 'workflow', 'task', workflow, task, '--jobname', name, '--load_inputs', inputs_dir, '--nocolor']

    if fork:
        cmd.append('--fork')
        cmd.append('--detach')

    if clean:
        if clean == 'recursive':
            cmd.append('--recursive_clean')
        else:
            cmd.append('--clean')

    proc = subprocess.run(
        cmd,
        capture_output=True,  # Capture both stdout and stderr
        text=True  # Automatically decode outputs to strings
        )
    shutil.rmtree(inputs_dir)
    if proc.returncode != 0:
        output = proc.stderr.strip()
        if output == '' :
            output = proc.stdout.strip()
        raise RuntimeError(output)  # Raise error with cleaned stderr content
    return proc.stdout.strip()

if __name__ == "__main__":
    import json
    res = run_job('Baking', 'bake_muffin_tray', 'test', add_blueberries=True, fork=True)
    print(res)
