from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
import pybind11

ext_modules = [
    Extension(
        "DT_user_param",
        ["DT_user_param.cpp"],
        include_dirs=[pybind11.get_include()],
        language="c++",
        extra_compile_args=["-O3", "-std=c++17"],
    ),
]

setup(
    name="DT_user_param",
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
)