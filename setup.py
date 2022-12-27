from setuptools import find_packages, setup

DEPS = ["PyYAML"]

setup(
    name="reflinkcep",
    version="0.1",
    author="Xuandeng Fu",
    author_email="fuxd@ios.ac.cn",
    packages=find_packages(),
    install_requires=DEPS,
)
