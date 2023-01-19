from setuptools import setup

setup(
    name='starvers',
    version='0.8.1',    
    description='Starvers is a python module for timestamp-based versioning of RDF data.',
    url='https://github.com/GreenfishK/starvers',
    author='Filip Kovacevic',
    author_email='filip.kovacevic@tuwien.ac.at',
    license='Apache License 2.0',
    install_requires=['pandas==1.3.4','pytest==7.1.3','rdflib==6.2.0', 'setuptools==65.4.0',
    'SPARQLWrapper==2.0.0','tzlocal==4.2'],

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',  
        'Operating System :: OS Independent',      
        'Programming Language :: Python :: 3.8',
    ],

    packages=['starvers'],
    package_dir={'':'src'},
    package_data={'starvers': ['templates/*.txt',
                               'templates/test_connection/*.txt']}
)