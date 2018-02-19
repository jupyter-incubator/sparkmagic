import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.FileInputStream;
import java.io.File;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.thrift.TException;

class RemoteHiveMeta {
    HiveMetaStoreClient client = null;
    List<String> dbs = null;

    public RemoteHiveMeta() {
        HiveConf conf = new HiveConf();
        //File initialFile = new File("/etc/hive-1.2.1/hive-site.xml");
        //FileInputStream targetStream = new FileInputStream(initialFile);
        //conf.addResource(targetStream);
        conf.addResource(new Path("file:///etc/hive-1.2.1/hive-site.xml"));
        try {
            this.client = new HiveMetaStoreClient(conf);
        }
        catch (MetaException e) {
            System.err.printf("Failed to connect to hive metastore: %s%n", e);
            System.exit(0);
        }
        this.setDatabases();
    }

    public List<String> getDatabases() {
        List<String> out = null;
        try {
            out = this.client.getAllDatabases();
        }
        catch (MetaException e) {
            System.err.printf("Failed to gather databases with: %s%n", e);
            System.exit(0);
        }
        return out;
    }

    public List<String> getAllTables(String database) {
        List<String> out = null;
        try {
            out = this.client.getAllTables(database);
        }
        catch (MetaException e) {
            System.err.printf("Failed to gather tables with: %s%n", e);
            System.exit(0);
        }
        return out;
    }

    public List<FieldSchema> getSchema(String table, String database) {
        List<FieldSchema> out = null;
        try {
            out = this.client.getSchema(table, database);
        }
        catch (TException e) {
            System.err.printf("Failed to gather schema with: %s%n", e);
            System.exit(0);
        }
        return out;
    }

    public void setDatabases() {
        this.dbs = this.getDatabases();
    }

    public List<String> getDescription(String table, String database) {
        List<FieldSchema> fields = this.getSchema(table, database);
        List<String> description = new ArrayList<String>();
        fields.forEach(f -> description.add(f.getName()));
        return description;
    }

    public Map<String, List<String>> getAllTables() {
        Map<String, List<String>> dtbs = new HashMap<String, List<String>>();
        this.dbs.forEach(db -> dtbs.put(db, this.getAllTables(db)));
        return dtbs;
    }

    public List<String> getFullyQualifiedTablesNames() {
        Map<String, List<String>> dtbs = this.getAllTables();
        List<String> tables = new ArrayList<String>();
        dtbs.forEach((db, tbs) -> {
                tbs.forEach(tb -> tables.add(String.format("%s.%s", db, tb)));
            }
        );
        return tables;
    }

    public void printDatabases() {
        System.out.println(String.join("\n", this.dbs));
    }

    public void printTables() {
        System.out.println(String.join("\n", this.getFullyQualifiedTablesNames()));
    }

    public void printTables(String database) {
        System.out.println(String.join("\n", this.getAllTables(database)));
    }

    public void printDescription(String table, String database) {
        System.out.println(String.join("\n", this.getDescription(table, database)));
    }

    public void closeClient() {
        this.client.close();
    }

    public static void main(String[] args) throws Exception {
        RemoteHiveMeta rm = new RemoteHiveMeta();
        try {
            if (args[0].equals("D:all"))
                rm.printDatabases();
            else if (args[0].equals("T:all"))
                rm.printTables();
            else if (args[0].equals("D:spec"))
                rm.printTables(args[1]);
            else if (args[0].equals("T:spec"))
                rm.printDescription(args[1], args[2]);
            else
                System.err.printf("Unrecognized format '%s'%n", args[0]);
        }
        catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("Not enough input arguments");
            System.exit(0);
        }
        rm.closeClient();        
    }
}
