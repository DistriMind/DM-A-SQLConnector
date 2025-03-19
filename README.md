[![CodeQL](https://github.com/DistriMind/DM-A-SQLConnector/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/DistriMind/DM-A-SQLConnector/actions/workflows/codeql-analysis.yml)

# DM-A-SQLConnector

DM-A-SQLConnector is a JDBC driver for Android's sqlite database (android.database.sqlite.SQLiteDatabase) originally conceived by Kristian Lein-Mathisen with the project name ASQLConnector. 

DM-A-SQLConnector lets you access your app's database through JDBC. Android ships with the necessary interfaces needed to use JDBC drivers, but it does not officially ship with a driver for its built-in SQLite database engine.  When porting code from other projects, you can conveniently replace the JDBC url to jdbc:sqlite to access an SQLite database on Android.

# How to use it ?

## Download with Gradle :

- Adapt into your build.gradle file, the next code (minimum Java version is 11) :
  ```
     ...
     dependencies {
         ...
         api(group:'fr.distrimind.oss.asqlconnector', name: 'DM-A-SQLConnector', version: '1.0.0-STABLE')
         ...
     }
     ...
  ```

- Libraries are available on Maven Central. You can check signatures of dependencies with this [public GPG key](key-2023-10-09.pub). You can also use the next repository :
   ```
       ...
       repositories {
           ...
           maven {
                   url "https://artifactory.distri-mind.fr/ui/native/gradle-release/"
           }
           ...
       }
       ...
   ```

To know what is the last uploaded version, please refer to versions available here : [this repository](https://artifactory.distri-mind.fr/artifactory/DistriMind-Public/fr/distrimind/oss/util)
## Download with Maven :
- Adapt into your pom.xml file, the next code (minimum Java version is 11) :
   ```
       ...
       <project>
           ...
           <dependencies>
               ...
               <dependency>
                   <groupId>fr.distrimind.oss.asqlconnector</groupId>
                   <artifactId>DM-A-SQLConnector</artifactId>
                   <version>1.0.0-STABLE</version>
               </dependency>  
               ...
           </dependencies>
           ...
       </project>
       ...
   ```

- Libraries are available on Maven Central. You can check signatures of dependencies with this [public GPG key](key-2023-10-09.pub). You can also use the next repository :
   ```
       ...
       <repositories>
           ...
           <repository>
               <id>DistriMind-Public</id>
               <url>https://artifactory.distri-mind.fr/ui/native/gradle-release/</url>
           </repository>
           ...
       </repositories>
       ...		
   ```
To know what last version has been uploaded, please refer to versions available into [this repository](https://artifactory.distri-mind.fr/artifactory/DistriMind-Public/fr/distrimind/oss/util)

## Usage

Here is a minimal example of an Android Activity implemented in Java with DM-A-SQLConnector.

```java
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;

public class MainActivity extends AppCompatActivity {

    private Connection connection;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        try {
            DriverManager.registerDriver((Driver) Class.forName("fr.distrimind.oss.asqlconnector.ASQLConnectorDriver").getConstructor().newInstance());
        } catch (Exception e) {
            throw new RuntimeException("Failed to register ASQLConnectorDriver");
        }
        String jdbcUrl = "jdbc:asqlconnector:" + "/data/data/" + getPackageName() + "/my-database.db";
        try {
            this.connection = DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onDestroy() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        super.onDestroy();
    }
}
```

You can find an example of how to use DM-A-SQLConnector with ActiveRecord on Ruboto here:

https://github.com/ruboto/ruboto/wiki/Tutorial%3A-Using-an-SQLite-database-with-ActiveRecord

## Debug output

You can set the DM-A-SQLConnector log output level like this

    fr.distrimind.oss.asqlconnector.Log.LEVEL = android.util.Log.VERBOSE;

You can turn on resultset dumps like this

    fr.distrimind.oss.asqlconnector.ASQLConnectorResultSet.dump = true;


# License

This code is free software: you can redistribute it and/or modify it under the terms of the MIT License
