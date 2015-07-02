# coding: utf-8
from setuptools import setup

setup(
    name='graphite-kudu',
    version='0.0.1',
    license='BSD',
    author=u'Dmitry Gryzunov',
    description=('A plugin for using graphite-web with Kudu as a backend'),
    py_modules=('kudu.kudu_graphite',),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    classifiers=(
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Topic :: System :: Monitoring',
    ),
    install_requires=(
        'python-kudu',
    ),
)
