BabuDB - Java Components
========================

```XML
<!-- Consider putting the repositories under a profile in your settings.xml -->
<repositories>
  <repository>
    <id>central</id>
    <url>http://repo.maven.apache.org/maven2</url>
  </repository>
  
  <repository>
    <id>xtreemfs-repository</id>
    <url>https://xtreemfs.github.io/xtreemfs</url>
    <snapshots>
      <enabled>true</enabled>
    </snapshots>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>org.xtreemfs.babudb</groupId>
    <artifactId>babudb-core</artifactId>
    <version>0.6.0-SNAPSHOT</version>
    <!-- The shaded version excludes the org.xtreemfs.babudb.sandbox -->
    <!-- package and includes applicable licenses.                   -->
    <!-- <classifier>shaded</classifier>                             -->
  </dependency>
  
  <dependency>
    <groupId>org.xtreemfs.babudb</groupId>
    <artifactId>babudb-replication</artifactId>
    <version>0.6.0-SNAPSHOT</version>
    <!-- The shaded version bundles:               -->
    <!-- - com.google.protobuf:protobuf-java       -->
    <!-- - org.xtreemfs.xtreemfs:xtreemfs-flease   -->
    <!-- - org.xtreemfs.xtreemfs:xtreemfs-pbrpcgen -->
    <!-- and includes applicable licenses.         -->
    <!-- <classifier>shaded</classifier>           -->
  </dependency>
</dependencies>
```
