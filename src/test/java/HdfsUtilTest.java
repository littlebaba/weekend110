import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * @author Li on 2018/2/28.
 */
public class HdfsUtilTest extends TestCase {

    HdfsUtil hu = new HdfsUtil();
    public void testUpload() throws IOException {
        hu.upload();
    }

    public void testUpload2() throws Exception {
        hu.init();
        hu.upload2();
    }

    public void testDownload() throws Exception {
        hu.download();
    }

    public void testMkdir() throws Exception {
        hu.mkdir();
    }

    public void testRm() throws Exception {
        hu.rm();
    }

    public void testListFiles() throws Exception {
        hu.listFiles();
    }
}