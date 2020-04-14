import setuptools

with open("README.md", 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='pycacd',
    version="0.4.0",
    author='Luis Manuel Torres Trevino',
    author_email='lmtorres123@icloud.com',
    description='Function library to track changes in SQL server and MySQL databases.',
    long_description=long_description,
    url="https://github.com/trevino-676/pyCaDc",
    packages=['py_cadc'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3.0",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'pyodbc',
        'mysql-replication'
    ]
)
