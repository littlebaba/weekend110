import junit.framework.TestCase;

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
}