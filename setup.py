import os
from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

c_collections = Extension('cgraph.c_collections', [os.path.join('cgraph', 'c_collections.pyx')])
dagraph = Extension('cgraph.dagraph', [os.path.join('cgraph', 'dagraph.pyx')])
iterators = Extension('cgraph.iterators', [os.path.join('cgraph', 'iterators.pyx')])

setup(
    cmdclass = {'build_ext': build_ext},
    ext_modules = [c_collections, dagraph, iterators],
    package_dir = {'cgraph': './cgraph'},
    packages = ['cgraph'],)
