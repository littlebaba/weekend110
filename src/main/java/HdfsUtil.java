import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @author Li on 2018/2/28.
 */
public class HdfsUtil {

    FileSystem fs=null;

    public void init() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://weekend110:9000/");
        fs = FileSystem.get(new URI("hdfs://weekend110:9000/"),conf,"hadoop");
    }

    /**
     * 上传文件
     * @throws IOException
     */
    public void upload() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://weekend110:9000/");
        FileSystem fs = FileSystem.get(conf);
        Path dst = new Path("hdfs://weekend110:9000/aa/qingshu.txt");
        FSDataOutputStream os = fs.create(dst);
        FileInputStream is = new FileInputStream("E:/qingshu.txt");
        IOUtils.copyBytes(is,os,1024);
    }

    public void upload2() throws Exception {
        init();
        fs.copyFromLocalFile(new Path("E:/qingshu.txt"),new Path("hdfs://weekend110:9000/aa/qingshu2.txt"));
    }
}



















