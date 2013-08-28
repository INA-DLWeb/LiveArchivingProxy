LiveArchivingProxy
==================

An HTTP Proxy that archives all intercepted trafic.

The Live Archiving Proxy (LAP) project is an HTTP proxy that is able to capture the traffic that flows through it. 
The LAP delegates the handling of the captured data to one or multiple writers using a simple network protocol. 
Writers exists for the DAFF, [WARC](http://www.digitalpreservation.gov/formats/fdd/fdd000236.shtml) and [ARC](http://www.digitalpreservation.gov/formats/fdd/fdd000235.shtml) format.
Using an HTTP proxy for Web archiving enables the use of any HTTP client for crawling ([Heritrix](http://github.com/internetarchive/heritrix3),
[PhantomJS](http://phantomjs.org/), [HTTrack](http://www.httrack.com/), [Scrapy](http://scrapy.org/), etc.) while keeping a unified and simple storage backend. 
The LAP is designed to be highly performant, easy to use and archive-format agnostic. It will run on any 64-bit linux system.

[Ina](http://www.ina.fr) uses the LAP in production since 2012 for 50% of its crawls and plans to use if for 100% of its crawls by 2014.


Getting started
---------------

 * User manual : https://github.com/INA-DLWeb/LiveArchivingProxy/raw/master/LAP-UserGuide.pdf
 * LAP binary : https://github.com/INA-DLWeb/LiveArchivingProxy/raw/master/lap.tar.gz
 * WARC writer : https://oss.sonatype.org/content/repositories/snapshots/fr/ina/dlweb/lap-writer-warc/1.0-SNAPSHOT/lap-writer-warc-1.0-SNAPSHOT-jar-with-dependencies.jar

Code resources
--------------
 * WARC writer project : https://bitbucket.org/nclarkekb/lap-writer-warc/
 * Generic writer project : https://oss.sonatype.org/content/repositories/snapshots/fr/ina/dlweb/lap-writer-generic/


