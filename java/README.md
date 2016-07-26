BabuDB - Java Components
========================

In your `$HOME/.m2/settings.xml` add:
```XML
<settings>
  <profiles>

    <!-- more profiles -->
  
    <profile>
      <id>babudb-dev</id>
      <repositories>
        <repository>
          <id>central</id>
          <url>http://repo.maven.apache.org/maven2</url>
        </repository>

        <repository>
          <id>xtreemfs-repository</id>
          <url>https://xtreemfs.github.io/xtreemfs/maven</url>
          <snapshots>
            <enabled>true</enabled>
          </snapshots>
        </repository>
      </repositories>
    </profile>
  
    <!-- more profiles -->
  
  </profiles>
</settings>
```

In your `pom.xml` add:
```XML
<project>

  <!-- more project configuration -->

  <dependencies>
    <dependency>
      <groupId>org.xtreemfs.babudb</groupId>
      <artifactId>babudb-core</artifactId>
      <version>0.5.6</version>
      <!-- The shaded version excludes the org.xtreemfs.babudb.sandbox -->
      <!-- package and includes applicable licenses.                   -->
      <!-- <classifier>shaded</classifier>                             -->
    </dependency>

    <dependency>
      <groupId>org.xtreemfs.babudb</groupId>
      <artifactId>babudb-replication</artifactId>
      <version>0.5.6</version>
      <!-- The shaded version bundles:                                                 -->
      <!-- - com.google.protobuf:protobuf-java                                         -->
      <!-- - org.xtreemfs.xtreemfs:xtreemfs-flease                                     -->
      <!-- - org.xtreemfs.xtreemfs:xtreemfs-foundation/org.xtreemfs.foundation.pbrpc.* -->
      <!-- and includes applicable licenses.                                           -->
      <!-- <classifier>shaded</classifier>                                             -->
    </dependency>
  </dependencies>

  <!-- more project configuration -->

</project>
```

And build your project like so:
```Bash
  mvn install -Pbabudb-dev
```
