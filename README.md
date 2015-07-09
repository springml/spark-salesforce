#Add to local repository

     mvn install:install-file -Dfile=lib/force-wsc-34.0.0-uber.jar -DgroupId=com.force.api \
    -DartifactId=force-wsc-uber -Dversion=34.0 -Dpackaging=jar


     mvn install:install-file -Dfile=lib/partner.jar -DgroupId=com.force.api \
    -DartifactId=force-partner-api-uber -Dversion=34.0 -Dpackaging=jar


#Create jar

       mvn clean install

#Running from cmd

    java -cp target/springml-salesforce.jar com.springml.spark.salesforce.examples.CsvExample local src/main/resources/sales.csv

# IDE

    Load code as Maven project in IDEA
