package org.apache.nifi.processors.pravega;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


public class TestPublishPravega {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(PublishPravega.class);
    }

    @Test
    @Ignore()
    public void testProcessor() {
        // TODO: write tests
    }

}
