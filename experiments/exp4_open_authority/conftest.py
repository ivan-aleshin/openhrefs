import pathlib
import sys

# This dir has an __init__.py, so pytest (prepend import mode + repo-root rootdir) would import
# the tests as `experiments.exp4_open_authority.*` and bare sibling imports (`from build_v3_map
# import ...`) would fail. Put the dir on sys.path so siblings resolve top-level.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
