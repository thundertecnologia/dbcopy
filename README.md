![TravisCI](https://api.travis-ci.org/thundertecnologia/dbcopy.svg?branch=master)

# dbcopy

Simple copy tables/data from one database to another. 
Today, it's converting only a MySql (source) to a SQLite database (destination).


### Use:
```
dbcopy.exe copy -s "server=<host>;uid=<user>;pwd=<password>;database=<dbname>;" -d "Data Source=<path to dbfile>" -L -v
```
or

```
dotnet dbcopy.dll copy -s "server=<host>;uid=<user>;pwd=<password>;database=<dbname>;" -d "Data Source=<path to dbfile>" -L -v
```

###### Options:

```
Usage: dbcopy copy [options]

Options:
  -?|-h|--help                         Show help information
  -s|--source <connectionString>       Specify source connection.
  -st|--source-type <type>             Specify source type connection. (mysql) only by now.
  -d|--destination <connectionString>  Specify destination connection.
  -dt|--destination-type <type>        Specify destination type connection. (sqlite) only by now.
  -L|--load-data                       Load data from source to destination.
  -v                                   Log level information.
```


We try to use https://github.com/dumblob/mysql2sqlite, but do not work correctly on some databases. So we decide to write similar functionality.

We try to use [sequel](http://sequel.jeremyevans.net/), with no sucess too.
