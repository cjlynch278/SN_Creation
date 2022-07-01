# QueryAndPushToCollibra
## Overview
This program query's the Service Now system applications in SQL and creates identical system applications in Collibra. The objects are determined from a SQL query defined
in the config yaml file. If no resutls found from the SQL Query, then no objects are created in Collibra.
## Instruction For Use

### Setup
The source executable file is kept in Artifact of each build. This folder contains a main.exe file and all of its dependencies. You will need add a 'config.yml' file into the root of this folder. This config file can be found within the source code of the project and should be structured like so:
```yaml  
...
  SQL_QUERY: <A SQL Query who's result will be pushed to Collibra>
...

```
### Running
Open a cmd prompt in the root of the project folder. Type main.exe and hit enter. The program will start to run and log its progress. During the run, the program will create the Collibra objects.





