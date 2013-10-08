HBase Storage Module for Thumbor
===================================

Introduction
------------

[Thumbor](https://github.com/globocom/thumbor/wiki) is a smart imaging service. It enables on-demand crop, resizing and flipping of images.

  
[HBase](https://hbase.apache.org) is a column oriented database from the Hadoop ecosystem.
  

This module provide support for Hadoop HBase as a large auto replicant key/value backend storage for images in Thumbor.


Installation
------------

The current version of the module is **0.9**.

In order to install the HBase Storage Module for Thumbor, you have to install Hadoop / HBase ecosystem first. 

## Hadoop / HBase installation

The HBase Storage Module for Thumbor was originally developed and tested on a Cloudera CDH3 Hadoop on a Debian system. 


### Installation on Ubuntu/Debian Systems

You can follow the [CDH3 Installation Guide](https://ccp.cloudera.com/display/CDHDOC/CDH3+Installation#CDH3Installation-InstallingCDH3onUbuntuandDebianSystems) for Ubuntu/Debian Systems Systems and install the following packages :

	sudo apt-get install hadoop-0.20 hadoop-0.20-native 
	sudo apt-get install hadoop-hbase hadoop-hbase-thrift


### Installation on RedHat Systems

You can follow the [CDH3 Installation Guide](https://ccp.cloudera.com/display/CDHDOC/CDH3+Installation#CDH3Installation-InstallingCDH3OnRedHatcompatiblesystems) for RedHat Systems and install the following packages :

	sudo yum install hadoop-0.20 hadoop-0.20-native
	sudo yum install hadoop-hbase hadoop-hbase-thrift


## Thumbor installation

You have to install [Thumbor](https://github.com/globocom/thumbor) following the [Thumbor Installation Guide](https://github.com/globocom/thumbor/wiki/Installing)...


## HBase Storage Module installation

... and finally the HBase Storage Module :

	pip install thumbor_hbase
	
	
Usage
-----

Using it is simple, just change your configuration in thumbor.conf:

    HBASE_STORAGE_SERVER_HOST = "localhost"
    HBASE_STORAGE_SERVER_PORT = 9000
    HBASE_STORAGE_TABLE = "storage-table"
    HBASE_STORAGE_FAMILY = "storage-family"

If you want to use thumbor_hbase for loading original images, change your thumbor.conf to read:

    LOADER = "thumbor_hbase.loader"
    
If you want to use thumbor_hbase for storage of original images, change your thumbor.conf to read:

    STORAGE = "thumbor_hbase.storage"

Testing
-------

In order to execute [pyvows](http://heynemann.github.com/pyvows/) tests, you have to install pyvows :

	pip install pyvows 

and run tests with :

	pyvows vows
	

License
-------

	Licensed under the MIT license:
	http://www.opensource.org/licenses/mit-license
	Copyright (c) 2011 globo.com timehome@corp.globo.com
	
