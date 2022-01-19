package org.ai.wu.ac.at.tdbArchive.utils;

import org.ai.wu.ac.at.tdbArchive.api.TripleStore;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeMap;

public class DatasetUtils {
    private static final Logger logger = LogManager.getLogger(DatasetUtils.class);

    public void logDatasetInfos(TripleStore tripleStore, Long ingestionTime, String tripleStoreLoc,
                                String datasetLoc, String logFileLoc) throws FileNotFoundException {
        File datasetLogFileDir = new File(logFileLoc).getParentFile();
        long dirSize = FileUtils.sizeOfDirectory(new File(tripleStoreLoc));
        long rawDataFileSize = FileUtils.sizeOf(new File(datasetLoc));

        String datasetLogFile = datasetLogFileDir + "/dataset_infos.csv";
        File f = new File(datasetLogFile);
        PrintWriter pw;
        if ( f.exists() && !f.isDirectory() ) {
            pw = new PrintWriter(new FileOutputStream(datasetLogFile, true));
        }
        else {
            pw = new PrintWriter(datasetLogFile);
            pw.append("ds_name,rdf_store_name,raw_data_size_in_MB,triple_store_size_in_MB,ingestion_time_in_s\n");
        }

        pw.append(datasetLoc + "," + tripleStore.toString() + "," + rawDataFileSize/1000000+ ","
                + dirSize/1000000 + "," + ingestionTime +"\n");
        pw.close();
        logger.info(String.format("Writing dataset logs to directory: %s", datasetLogFile));
    }

}
