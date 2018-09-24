# wget https://waf.io/waf-2.0.11 -O waf
# chmod +x waf
# ./waf configure
# ./waf
import waflib
import os

APPNAME = "qtool"
VERSION = "0.1.0"

def configure(cnf):
    cnf.load("compiler_c")
    cnf.check(lib="pthread", uselib_store="PTHREAD")
    cnf.check(lib="dl", uselib_store="DL")
    cnf.check(lib="dill", uselib_store="DILL")
    cnf.check(lib="sqlite3", uselib_store="SQLITE")
    cnf.check(lib="xxhash", uselib_store="XXHASH")
    print("building cbindings for rust's params parser")
    cnf.cmd_and_log("cd csv; cargo build --release", stdout = waflib.Context.BOTH)

def options(opt):
    opt.load("compiler_c")

def build(bld):
    cflags = [
        "-Wall",
    ]

    if bld.variant == "debug":
        cflags.extend([
            "-DDEBUG=1",
            "-g",
        ])

    if bld.variant == "release":
        cflags.extend([
            "-O3",
        ])
            
    if bld.variant == "test":
        bld.add_post_fun(lambda b: b.cmd_and_log('python tests/test_qtool.py\n', stdout = waflib.Context.BOTH))

        return

    bld.program(
        target = "qtool",
        source = bld.path.ant_glob("src/**/*.c"),
        includes = "src",
        stlib = ['qparams'],
        stlibpath = [os.getcwd() + '/csv/target/release'],
        uselib = ["DILL", "SQLITE", "PTHREAD", "DL", "PARAMS", "XXHASH"],
        cflags = cflags
    )

from waflib.Build import BuildContext
class debug(BuildContext):
    cmd = "debug"
    variant = "debug"

class release(BuildContext):
    cmd = "release"
    variant = "release"

class test(BuildContext):
    cmd = "test"
    variant = "test"
