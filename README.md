# Files 2 SQLite (F2S)
Converts all files in one directory into a SQLite database table where the files are blob entries and their names form the primary key or an optional column. Allows foreign key index extraction of all single processed files.

# Run
## Jar
```
java -jar f2s.jar arguments_you_want_to_use
```

## Eclipse
Download latest version, open project in eclipse, create a run configuration with command line arguments, and hit run. Using the jar file without Eclipse is much faster.

# Arguments
Run:
```
java -jar f2s.jar -h
```
or look at the [commands array in the source code of F2S.java](https://github.com/ManuelTS/f2s/blob/master/src/F2S.java#L14).

# Build
Run
```
ant build
```
# Example
To create an `con.sqlite` database with a table `contacts` containing all the files from the directory `../Databases/contacts/` and an primary id column `id` starting from 0 in the current dirctory removing all uppercase characters except `M` and removing all `.` from the file names run:
```
java -jar f2s.jar -i ../Databases/contacts/ -n contacts -r [A-LN-Z|\\.]+ -p 0 -o ./con
```
