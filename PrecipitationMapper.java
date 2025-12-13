import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class PrecipitationMapper extends Mapper<LongWritable, Text, Text, Text> {

    private HashMap<String, String> locationMap = new HashMap<>();
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        String locationPath = conf.get("locationFile");

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path(locationPath));

        java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(in));
        String line;

        // Skip header
        br.readLine();

        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\t|,");
            if (parts.length >= 8) {
                String locationId = parts[0];
                String cityName = parts[7];
                locationMap.put(locationId, cityName);
            }
        }
        br.close();
    }

    @Override
 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        if (line.startsWith("location_id")) return;

        String[] f = line.split("\t|,");
        if (f.length < 14) return;

        String locationId = f[0];
        String date = f[1];
        String tempMean = f[5];
        String precipitation = f[13];

        if (!locationMap.containsKey(locationId)) return;

        String city = locationMap.get(locationId);

        String[] d = date.split("/");
        if (d.length != 3) return;

        String month = d[0];

        outKey.set(city + "," + month);
        outValue.set(precipitation + "," + tempMean);
        context.write(outKey, outValue);
    }
}


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
