![image](./logo.jpg)

A fluent-bit output plugin to send logs to database.

## Features
- [x] output to all kind of databases such as mysql, postgresql, clickhouse, oceanbase.
- [x] multiple instance is supported.
- [x] dynamic table schema is supported.

## Usage
the plugin configure in "/fluent-bit/etc" directory. The plugin need a configuration file named "fluent-bit.conf" and a plugin file named "plugins.conf".
+ fluent-bit.conf
```
[SERVICE]
    Flush           1
    Log_Level       info
    plugins_file    /fluent-bit/etc/plugins.conf

[INPUT]
    Name   dummy
    Tag    dummy
    dummy  {"data":"mussum ipsum, cacilds vidis litro abertis"}

[OUTPUT]
    Name database
    Dialect mysql
    address localhost:3306
    user root
    password my-secret-pw
    database fluent_bit
    table test
    batchSize 100
    ignoreColumns "id created_at updated_at"
```

+ plugins.conf
```
[PLUGINS]
    Path /fluent-bit/etc/database.so

```

## Build
after make, a file named 'database.so' in bin directory.
```shell
make all
```

## Details

+ Name: database, the plugin is registered with "database" name.
+ Dialect: the database type, now it supported mysql, postgresql, clickhouse, oceanbase. mysql is default.
+ address: the database address, such as "localhost:3306".
+ user: the database user.
+ password: the database password.
+ table: the table name which the data will be written to.
+ batchSize: the batch size of the data to be written to the database.
+ ignoreColumns: the database columns which will be ignored when writing data to the database. such as id, created_time, updated_time.




