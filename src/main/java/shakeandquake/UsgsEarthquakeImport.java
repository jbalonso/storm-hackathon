package shakeandquake;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.DefaultRetrier;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class UsgsEarthquakeImport {

    public void loadData() {
        try {
            PBClusterConfig clusterConfig = new PBClusterConfig(5);

            PBClientConfig clientConfig = new PBClientConfig.Builder()
                    .withPort(8087)
                    .build();

            String[] hosts = new String[] {
                    "cluster-7-slave-00.sl.hackreduce.net",
                    "cluster-7-slave-02.sl.hackreduce.net",
                    "cluster-7-slave-03.sl.hackreduce.net",
                    "cluster-7-slave-06.sl.hackreduce.net"
            };
            clusterConfig.addHosts(clientConfig, hosts);

            IRiakClient riakClient = RiakFactory.newClient(clusterConfig);

            Bucket bucket = riakClient.createBucket("shakeandquake")
                    .withRetrier(DefaultRetrier.attempts(3))
                    .execute();

            System.out.println("Removing old keys");
            for(String key : bucket.keys()) {
                bucket.delete(key).execute();
            }
            System.out.println("Finished removing old keys");

            System.out.println("Writing entries to Riak");
            String contents = new String(Files.toByteArray(new File("data/all_month.geojson")));

            ObjectMapper mapper = new ObjectMapper();
            Map<String,Object> userData = mapper.readValue(contents, Map.class);

            int counter = 0;
            List<Map<String, Object>> features = (List<Map<String, Object>>) userData.get("features");
            for(Map<String, Object> feature : features) {
                Map<String, Object> properties = (Map<String, Object>) feature.get("properties");
                Number magnitude = (Number) properties.get("mag");
                String time = properties.get("time").toString();

                if(magnitude.doubleValue() > 2.5) {
                    bucket.store(time, magnitude).execute();
                    counter++;
                }
            }
            System.out.println("Inserted " + counter + " objects to Riak");

            riakClient.shutdown();

        } catch (Exception e) {
            throw new RuntimeException("Exception while talking to Riak!", e);
        }
    }

    public static void main(String[] args) throws RiakException, IOException {
        UsgsEarthquakeImport importer = new UsgsEarthquakeImport();
        importer.loadData();
    }

}
