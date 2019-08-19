import sys
py_version = sys.version_info
major = py_version[0]
minor = py_version[1]
if not major > 2 and not minor > 6:
    sys.exit(1)