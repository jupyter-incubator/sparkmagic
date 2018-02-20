import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.FileInputStream;
import java.io.File;

import javax.jdo.JDOObjectNotFoundException;

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

    public RemoteHiveMeta(String hivexml) throws RMMException {
        HiveConf conf = new HiveConf();
        conf.addResource(new Path(String.format("file://%s", hivexml)));
        try {
            this.client = new HiveMetaStoreClient(conf);
        }
        catch (MetaException e) {
            throw new RMMException(String.format("Failed to connect to hive metastore: %s%n", e));
        }
        this.setDatabases();
    }

    public List<String> getDatabases() throws RMMException {
        List<String> out = null;
        try {
            out = this.client.getAllDatabases();
        }
        catch (MetaException e) {
            throw new RMMException(String.format("Failed to gatjer databases: %s%n", e));
        }
        return out;
    }

    public List<String> getAllTables(String database) throws RMMException {
        List<String> out = null;
        try {
            out = this.client.getAllTables(database);
        }
        catch (MetaException e) {
            throw new RMMException(String.format("Failed to gather tables with: %s%n", e));
        }
        return out;
    }

    public List<FieldSchema> getSchema(String table, String database) throws RMMException {
        List<FieldSchema> out = null;
        try {
            out = this.client.getSchema(table, database);
        }
        catch (TException e) {
            throw new RMMException(String.format("Failed to gather schema with: %s%n", e));
        }
        return out;
    }

    public void setDatabases() throws RMMException {
        this.dbs = this.getDatabases();
    }

    public List<String> getDescription(String table, String database) throws RMMException {
        List<FieldSchema> fields = this.getSchema(table, database);
        List<String> description = new ArrayList<String>();
        fields.forEach(f -> description.add(f.getName()));
        return description;
    }

    public Map<String, List<String>> getAllTables() throws RMMException {
        Map<String, List<String>> dtbs = new HashMap<String, List<String>>();
        for (String db : this.dbs) { 
            dtbs.put(db, this.getAllTables(db));
        }
        return dtbs;
    }

    public List<String> getFullyQualifiedTableNames() throws RMMException {
        Map<String, List<String>> dtbs = this.getAllTables();
        List<String> tables = new ArrayList<String>();
        dtbs.forEach((db, tbs) -> {
                tbs.forEach(tb -> tables.add(String.format("%s.%s", db, tb)));
            }
        );
        return tables;
    }

    public void printDatabases() throws RMMException {
        System.out.println(String.join("\n", this.dbs));
    }

    public void printTables() throws RMMException {
        System.out.println(String.join("\n", this.getFullyQualifiedTableNames()));
    }

    public void printTables(String database) throws RMMException {
        System.out.println(String.join("\n", this.getAllTables(database)));
    }

    public void printDescription(String table, String database) throws RMMException {
        System.out.println(String.join("\n", this.getDescription(table, database)));
    }

    public void closeClient() {
        this.client.close();
    }

    public static void main(String[] args) throws Exception {
        RemoteHiveMeta rm = null;
        String[] types = new String[]{"D:all", "T:all", "D:spec", "T:spec"};
        try {
            File f = new File(args[0]);
            if (!f.exists() || f.isDirectory()) { 
                System.err.printf("Could not hive-xml file: '%s'%n", args[0]);
                System.exit(0);
            }

            rm = new RemoteHiveMeta(args[0]);
            if (args[1].equals(types[0]))
                rm.printDatabases();
            else if (args[1].equals(types[1]))
                rm.printTables();
            else if (args[1].equals(types[2]))
                rm.printTables(args[2]);
            else if (args[1].equals(types[3]))
                rm.printDescription(args[2], args[3]);
            else {
                System.err.printf("Unrecognized format '%s'%n", args[1]);
                System.err.printf("Please use one of: [%s] %n", String.join(", ", types));
            }
        }
        catch (ArrayIndexOutOfBoundsException e) {
            System.err.printf("Not enough input arguments%nGot: (%s)%n", String.join(",", args));
            System.err.printf("Usage: <program>.java <hive-xml_path> <{%s}> [<additional_args>]%n", String.join(",", types));
        } catch (RMMException e) {
            // Do nothing. Let program exit..
            System.err.println(e.getMessage());
            System.err.println("HELLO");
        }
        rm.closeClient();        
    }

    private class RMMException extends Exception {
        public RMMException(String message) {
            super(message);
        }
    }
}
