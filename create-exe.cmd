@echo off
mkdir target
mkdir target\winbuild
copy cli\target\simplecsv-cli-1.0-SNAPSHOT-exec.jar target\winbuild

mkdir target\exe
jpackage --input target\winbuild ^
    --win-console ^
    --name simplecsv ^
    --main-jar simplecsv-cli-1.0-SNAPSHOT-exec.jar ^
    --app-version 1.0 ^
    --type app-image ^
    -d target\exe
