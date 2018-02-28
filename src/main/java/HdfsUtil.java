import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
        fs.copyFromLocalFile(new Path("E:/qingshu.txt"),new Path("hdfs://weekend110:9000/aaa/bbb/ccc/qingshu2.txt"));
    }

    /**
     * 下载文件
     * @throws Exception
     */
    public void download() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://weekend110:9000/");
        FileSystem fs = FileSystem.get(new URI("hdfs://weekend110:9000/"),conf,"hadoop");
//        fs.copyToLocalFile(new Path("hdfs://weekend110:9000/aa/qingshu2.txt"),new Path("E:/qingshu2.txt"));
        fs.copyToLocalFile(false,new Path("hdfs://weekend110:9000/aa/qingshu2.txt"),new Path("E:/qingshu2.txt"),true);
    }

    /**
     * 创建文件夹
     * @throws Exception
     */
    public void mkdir() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://weekend110:9000/");
        FileSystem fs = FileSystem.get(new URI("hdfs://weekend110:9000/"),conf,"hadoop");
        fs.mkdirs(new Path("/aaa/bbb/ccc"));
    }

    /**
     * 删除文件
     * @throws Exception
     */
    public void rm() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://weekend110:9000/");
        FileSystem fs = FileSystem.get(new URI("hdfs://weekend110:9000/"),conf,"hadoop");
        fs.delete(new Path("/aa"),true);
    }

    /**
     * 列出文件
     * @throws Exception
     */
    public void listFiles() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://weekend110:9000/");
        FileSystem fs = FileSystem.get(new URI("hdfs://weekend110:9000/"),conf,"hadoop");
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while (files.hasNext()){
            LocatedFileStatus file = files.next();
            Path filePath = file.getPath();
            String filename = filePath.getName();
            System.out.println(filename);
        }

        System.out.println("=========================");
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus status:listStatus) {
            String name = status.getPath().getName();
            System.out.println(name+(status.isDirectory()?" is dir":" is file"));
        }
    }
}



















