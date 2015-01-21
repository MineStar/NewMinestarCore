Minestar Core
=============
This is the core library for our plugins. The core itself has no game relevant functionality and will only provide
helpful classes for other plugins.

The project will provide implementations of the features in the **source** folder and examples for usage as JUnit tests
in the **test** folder.
Database
--------
**TODO**: Link to the package

Based on the ORM ORMLite http://ormlite.com/ the core provides a standard API to persist and read object to and from
a database.

There is also an API for a queue based persisting of objects for a higher output performance.