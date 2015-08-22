# SchemaToDDL

In order for oracle DDL extraction to work you will need to install ojdbc jar locally. This is the maven command to do it. 

    mvn install:install-file -Dfile=/tmp/ojdbc6.jar -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=11.2.0 -Dpackaging=jar
