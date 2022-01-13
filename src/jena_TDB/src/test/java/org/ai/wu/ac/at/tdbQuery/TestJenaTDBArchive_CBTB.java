/**
 * 
 */
package org.ai.wu.ac.at.tdbQuery;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.ai.wu.ac.at.tdbArchive.api.JenaTDBArchive;
import org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_CBTB;
import org.ai.wu.ac.at.tdbArchive.solutions.DiffSolution;
import org.ai.wu.ac.at.tdbArchive.utils.QueryUtils;
import org.junit.Test;

/**
 * @author Javier D. Fernández
 *
 */
public class TestJenaTDBArchive_CBTB {

	JenaTDBArchive jenaArchive;

	public TestJenaTDBArchive_CBTB() throws FileNotFoundException, InterruptedException, ExecutionException {

		// Start Archive
		jenaArchive = new JenaTDBArchive_CBTB();
		URL resourceUrl = this.getClass().getResource(FileSystems.getDefault().getSeparator() + "testCBTB/tdbTest");
		assertNotNull(resourceUrl);
		jenaArchive.load(resourceUrl.getFile(), null);
		// ArrayList<String> solution = jenaArchive.matQuery(1, "<http://example.org/uri3>");
	}

	@Test
	public void testSimpleMatInitialVersion() throws InterruptedException, ExecutionException {
		String query = QueryUtils.createLookupQueryGraph("subject", "<http://example.org/uri3>");
		ArrayList<String> solution = jenaArchive.matQuery(0, query);
		ArrayList<String> expected = new ArrayList<String>();
		expected.add("<http://example.org/predicate3> <http://example.org/uri4>");
		expected.add("<http://example.org/predicate3> <http://example.org/uri5>");
		//System.out.println(solution);
		assertTrue(expected.containsAll(solution) && solution.containsAll(expected));
	}

	@Test
	public void testSimpleMatEndVersion() throws InterruptedException, ExecutionException {
		String query = QueryUtils.createLookupQueryGraph("subject", "<http://example.org/uri4>");
		ArrayList<String> solution = jenaArchive.matQuery(3, query);
		ArrayList<String> expected = new ArrayList<String>();
		expected.add("<http://example.org/predicate4> <http://example.org/uri6>");
		//System.out.println(solution);
		assertTrue(expected.containsAll(solution) && solution.containsAll(expected));
	}

	@Test
	public void testSimpleMatMiddleVersion() throws InterruptedException, ExecutionException {
		String query = QueryUtils.createLookupQueryGraph("object", "\"literal3\"");
		ArrayList<String> solution = jenaArchive.matQuery(2, query);
		ArrayList<String> expected = new ArrayList<String>();
		expected.add("<http://example.org/uri1> <http://example.org/predicate1>");

		assertTrue(expected.containsAll(solution) && solution.containsAll(expected));
	}

	@Test
	public void testSimpleMatEndVersionLiterals() throws InterruptedException, ExecutionException {
		String query = QueryUtils.createLookupQueryGraph("predicate", "<http://example.org/predicate1>");
		ArrayList<String> solution = jenaArchive.matQuery(3, query);
		ArrayList<String> expected = new ArrayList<String>();
		expected.add("<http://example.org/uri1> literal4");

		assertTrue(expected.containsAll(solution) && solution.containsAll(expected));
	}

	@Test
	public void testbulkAllMatQuerying() throws InterruptedException, ExecutionException, FileNotFoundException, IOException {

		URL queryurl = this.getClass().getResource(FileSystems.getDefault().getSeparator() + "testCBTB/testQueryDynamic.txt");
		ArrayList<Map<Integer, ArrayList<String>>> solution = jenaArchive.bulkAllMatQuerying(queryurl.getFile(), "subject");

		ArrayList<String> version0 = new ArrayList<String>();
		version0.add("<http://example.org/predicate1> literal1");
		version0.add("<http://example.org/predicate1> literal2");
		version0.add("<http://example.org/predicate1> literal3");
		version0.add("<http://example.org/predicate1> literal4");
		version0.add("<http://example.org/predicate2> <http://example.org/uri2>");
		version0.add("<http://example.org/predicate2> <http://example.org/uri5>");
		//System.out.println(solution);
		ArrayList<String> solutionversion = solution.get(0).get(0);
		assertTrue(version0.containsAll(solutionversion) && solutionversion.containsAll(version0));

		ArrayList<String> version1 = new ArrayList<String>();
		version1.add("<http://example.org/predicate1> literal1");
		version1.add("<http://example.org/predicate1> literal2");
		version1.add("<http://example.org/predicate1> literal3");
		version1.add("<http://example.org/predicate2> <http://example.org/uri2>");
		version1.add("<http://example.org/predicate2> <http://example.org/uri5>");
		solutionversion = solution.get(0).get(1);
		assertTrue(version1.containsAll(solutionversion) && solutionversion.containsAll(version1));

		ArrayList<String> version2 = new ArrayList<String>();
		version2.add("<http://example.org/predicate1> literal1");
		version2.add("<http://example.org/predicate1> literal2");
		version2.add("<http://example.org/predicate1> literal3");
		version2.add("<http://example.org/predicate2> <http://example.org/uri2>");
		version2.add("<http://example.org/predicate2> <http://example.org/uri5>");
		solutionversion = solution.get(0).get(2);
		assertTrue(version2.containsAll(solutionversion) && solutionversion.containsAll(version2));

		ArrayList<String> version3 = new ArrayList<String>();
		version3.add("<http://example.org/predicate1> literal4");
		version3.add("<http://example.org/predicate2> <http://example.org/uri2>");
		version3.add("<http://example.org/predicate2> <http://example.org/uri5>");
		solutionversion = solution.get(0).get(3);
		assertTrue(version3.containsAll(solutionversion) && solutionversion.containsAll(version3));
	}
	@Test
	public void testbulkAllMatQuerying_SP_Pattern() throws InterruptedException, ExecutionException, FileNotFoundException, IOException {

		URL queryurl = this.getClass().getResource(FileSystems.getDefault().getSeparator() + "testCB/testQueryDynamic_SP.txt");
		ArrayList<Map<Integer, ArrayList<String>>> solution = jenaArchive.bulkAllMatQuerying(queryurl.getFile(), "SP");

		ArrayList<String> version0 = new ArrayList<String>();
		version0.add("<http://example.org/uri2>");
		version0.add("<http://example.org/uri5>");
		ArrayList<String> solutionversion = solution.get(0).get(0);
		
		assertTrue(version0.containsAll(solutionversion) && solutionversion.containsAll(version0));

		ArrayList<String> version1 = new ArrayList<String>();
		version1.add("<http://example.org/uri2>");
		version1.add("<http://example.org/uri5>");
		solutionversion = solution.get(0).get(1);
		assertTrue(version1.containsAll(solutionversion) && solutionversion.containsAll(version1));

		ArrayList<String> version2 = new ArrayList<String>();
		version2.add("<http://example.org/uri2>");
		version2.add("<http://example.org/uri5>");
		solutionversion = solution.get(0).get(2);
		assertTrue(version2.containsAll(solutionversion) && solutionversion.containsAll(version2));

		ArrayList<String> version3 = new ArrayList<String>();
		version3.add("<http://example.org/uri2>");
		version3.add("<http://example.org/uri5>");
		solutionversion = solution.get(0).get(3);
		assertTrue(version3.containsAll(solutionversion) && solutionversion.containsAll(version3));
	}
	@Test
	public void testbulkAllMatQuerying_SO_Pattern() throws InterruptedException, ExecutionException, FileNotFoundException, IOException {

		URL queryurl = this.getClass().getResource(FileSystems.getDefault().getSeparator() + "testCB/testQueryDynamic_SO.txt");
		ArrayList<Map<Integer, ArrayList<String>>> solution = jenaArchive.bulkAllMatQuerying(queryurl.getFile(), "SO");

		ArrayList<String> version0 = new ArrayList<String>();
		version0.add("<http://example.org/predicate1>");
		ArrayList<String> solutionversion = solution.get(0).get(0);
		
		assertTrue(version0.containsAll(solutionversion) && solutionversion.containsAll(version0));

		ArrayList<String> version1 = new ArrayList<String>();
		version1.add("<http://example.org/predicate1>");
		solutionversion = solution.get(0).get(1);
		assertTrue(version1.containsAll(solutionversion) && solutionversion.containsAll(version1));

		ArrayList<String> version2 = new ArrayList<String>();
		version2.add("<http://example.org/predicate1>");
		solutionversion = solution.get(0).get(2);
		assertTrue(version2.containsAll(solutionversion) && solutionversion.containsAll(version2));

		ArrayList<String> version3 = new ArrayList<String>();
		solutionversion = solution.get(0).get(3);
		assertTrue(version3.containsAll(solutionversion) && solutionversion.containsAll(version3));
	}
	@Test
	public void testbulkAllMatQuerying_PO_Pattern() throws InterruptedException, ExecutionException, FileNotFoundException, IOException {

		URL queryurl = this.getClass().getResource(FileSystems.getDefault().getSeparator() + "testCB/testQueryDynamic_PO.txt");
		ArrayList<Map<Integer, ArrayList<String>>> solution = jenaArchive.bulkAllMatQuerying(queryurl.getFile(), "PO");

		ArrayList<String> version0 = new ArrayList<String>();
		version0.add("<http://example.org/uri1>");
		version0.add("<http://example.org/uri2>");
		ArrayList<String> solutionversion = solution.get(0).get(0);
		assertTrue(version0.containsAll(solutionversion) && solutionversion.containsAll(version0));

		ArrayList<String> version1 = new ArrayList<String>();
		version1.add("<http://example.org/uri1>");
		solutionversion = solution.get(0).get(1);
		assertTrue(version1.containsAll(solutionversion) && solutionversion.containsAll(version1));

		ArrayList<String> version2 = new ArrayList<String>();
		version2.add("<http://example.org/uri1>");
		solutionversion = solution.get(0).get(2);
		assertTrue(version2.containsAll(solutionversion) && solutionversion.containsAll(version2));

		ArrayList<String> version3 = new ArrayList<String>();
		solutionversion = solution.get(0).get(3);
		
		assertTrue(version3.containsAll(solutionversion) && solutionversion.containsAll(version3));
	}
	@Test
	public void testbulkAllMatQuerying_SPO_Pattern() throws InterruptedException, ExecutionException, FileNotFoundException, IOException {

		URL queryurl = this.getClass().getResource(FileSystems.getDefault().getSeparator() + "testCB/testQueryDynamic_SPO.txt");
		ArrayList<Map<Integer, ArrayList<String>>> solution = jenaArchive.bulkAllMatQuerying(queryurl.getFile(), "SPO");

		ArrayList<String> version0 = new ArrayList<String>();
		version0.add("true");
		ArrayList<String> solutionversion = solution.get(0).get(0);
		//System.out.println(solutionversion);
		assertTrue(version0.containsAll(solutionversion) && solutionversion.containsAll(version0));

		ArrayList<String> version1 = new ArrayList<String>();
		version1.add("true");
		solutionversion = solution.get(0).get(1);
		assertTrue(version1.containsAll(solutionversion) && solutionversion.containsAll(version1));

		ArrayList<String> version2 = new ArrayList<String>();
		version2.add("true");
		solutionversion = solution.get(0).get(2);
		assertTrue(version2.containsAll(solutionversion) && solutionversion.containsAll(version2));

		ArrayList<String> version3 = new ArrayList<String>();
		version3.add("false");
		solutionversion = solution.get(0).get(3);
		
		assertTrue(version3.containsAll(solutionversion) && solutionversion.containsAll(version3));
	}
	@Test
	public void testbulkAlldiffQueryingwithDeletes_SO() throws InterruptedException, ExecutionException, IOException {
		URL queryurl = this.getClass().getResource(FileSystems.getDefault().getSeparator() + "testCB/testQueryDynamic_SO.txt");

		ArrayList<Map<Integer, DiffSolution>> solution = jenaArchive.bulkAlldiffQuerying(queryurl.getFile(), "SO", 1);

		Map<Integer, DiffSolution> solutionFirstLine = solution.get(0);
		/*
		 * diff 0 and 1
		 */
		ArrayList<String> solutionAdd = solutionFirstLine.get(1).getAdds();

		ArrayList<String> solutionDel = solutionFirstLine.get(1).getDels();

		ArrayList<String> expectedDel = new ArrayList<String>();
		ArrayList<String> expectedAdd = new ArrayList<String>();

		
		// System.out.println(solutionDel);
		assertTrue(expectedAdd.containsAll(solutionAdd) && solutionAdd.containsAll(expectedAdd));
		assertTrue(expectedDel.containsAll(solutionDel) && solutionDel.containsAll(expectedDel));

		/*
		 * diff 0 and 2
		 */
		solutionAdd = solutionFirstLine.get(2).getAdds();
		solutionDel = solutionFirstLine.get(2).getDels();

		expectedDel = new ArrayList<String>();
		expectedAdd = new ArrayList<String>();

		
		// System.out.println(solutionDel);
		assertTrue(expectedAdd.containsAll(solutionAdd) && solutionAdd.containsAll(expectedAdd));
		assertTrue(expectedDel.containsAll(solutionDel) && solutionDel.containsAll(expectedDel));

		/*
		 * diff 0 and 3
		 */
		solutionAdd = solutionFirstLine.get(3).getAdds();
		// System.out.println(solutionAdd);
		solutionDel = solutionFirstLine.get(3).getDels();
		 //System.out.println(solutionDel);

		expectedDel = new ArrayList<String>();
		expectedAdd = new ArrayList<String>();

		expectedDel.add("<http://example.org/predicate1>");
		assertTrue(expectedAdd.containsAll(solutionAdd) && solutionAdd.containsAll(expectedAdd));
		assertTrue(expectedDel.containsAll(solutionDel) && solutionDel.containsAll(expectedDel));

	}
	@Test
	public void testbulkAlldiffQueryingwithDeletes_SPO() throws InterruptedException, ExecutionException, IOException {
		URL queryurl = this.getClass().getResource(FileSystems.getDefault().getSeparator() + "testCB/testQueryDynamic_SPO.txt");

		ArrayList<Map<Integer, DiffSolution>> solution = jenaArchive.bulkAlldiffQuerying(queryurl.getFile(), "SPO", 1);

		Map<Integer, DiffSolution> solutionFirstLine = solution.get(0);
		/*
		 * diff 0 and 1
		 */
		ArrayList<String> solutionAdd = solutionFirstLine.get(1).getAdds();

		ArrayList<String> solutionDel = solutionFirstLine.get(1).getDels();

		ArrayList<String> expectedDel = new ArrayList<String>();
		ArrayList<String> expectedAdd = new ArrayList<String>();

		
		// System.out.println(solutionDel);
		assertTrue(expectedAdd.containsAll(solutionAdd) && solutionAdd.containsAll(expectedAdd));
		assertTrue(expectedDel.containsAll(solutionDel) && solutionDel.containsAll(expectedDel));

		/*
		 * diff 0 and 2
		 */
		solutionAdd = solutionFirstLine.get(2).getAdds();
		solutionDel = solutionFirstLine.get(2).getDels();

		expectedDel = new ArrayList<String>();
		expectedAdd = new ArrayList<String>();

		
		// System.out.println(solutionDel);
		assertTrue(expectedAdd.containsAll(solutionAdd) && solutionAdd.containsAll(expectedAdd));
		assertTrue(expectedDel.containsAll(solutionDel) && solutionDel.containsAll(expectedDel));

		/*
		 * diff 0 and 3
		 */
		solutionAdd = solutionFirstLine.get(3).getAdds();
		 //System.out.println(solutionAdd);
		solutionDel = solutionFirstLine.get(3).getDels();
		 //System.out.println(solutionDel);

		expectedDel = new ArrayList<String>();
		expectedAdd = new ArrayList<String>();

		expectedDel.add("true");
		expectedAdd.add("false");
		assertTrue(expectedAdd.containsAll(solutionAdd) && solutionAdd.containsAll(expectedAdd));
		assertTrue(expectedDel.containsAll(solutionDel) && solutionDel.containsAll(expectedDel));

	}
	@Test
	public void testBulkAllVerQuerying_PO() throws InterruptedException, ExecutionException, FileNotFoundException, IOException {

		URL queryurl = this.getClass().getResource(FileSystems.getDefault().getSeparator() + "testCB/testQueryDynamic_PO.txt");

		ArrayList<Map<Integer, ArrayList<String>>> solution = jenaArchive.bulkAllVerQuerying(queryurl.getFile(), "PO");

		Map<Integer, ArrayList<String>> solutionFirstLine = solution.get(0);

		ArrayList<String> version0 = new ArrayList<String>();
		version0.add("<http://example.org/uri1>");
		version0.add("<http://example.org/uri2>");
		ArrayList<String> solutionversion = solutionFirstLine.get(0);
		assertTrue(version0.containsAll(solutionversion) && solutionversion.containsAll(version0));

		ArrayList<String> version1 = new ArrayList<String>();
		version1.add("<http://example.org/uri1>");
		solutionversion = solutionFirstLine.get(1);
		assertTrue(version1.containsAll(solutionversion) && solutionversion.containsAll(version1));

		ArrayList<String> version2 = new ArrayList<String>();
		version2.add("<http://example.org/uri1>");
		solutionversion = solutionFirstLine.get(2);
		assertTrue(version2.containsAll(solutionversion) && solutionversion.containsAll(version2));

		ArrayList<String> version3 = new ArrayList<String>();
	
		solutionversion = solutionFirstLine.get(3);
		assertTrue(version3.containsAll(solutionversion) && solutionversion.containsAll(version3));
	}
	@Test
	public void testBulkAllVerQuerying_SPO() throws InterruptedException, ExecutionException, FileNotFoundException, IOException {

		URL queryurl = this.getClass().getResource(FileSystems.getDefault().getSeparator() + "testCB/testQueryDynamic_SPO.txt");

		ArrayList<Map<Integer, ArrayList<String>>> solution = jenaArchive.bulkAllVerQuerying(queryurl.getFile(), "SPO");

		Map<Integer, ArrayList<String>> solutionFirstLine = solution.get(0);

		ArrayList<String> version0 = new ArrayList<String>();
		version0.add("true");
		ArrayList<String> solutionversion = solutionFirstLine.get(0);
		assertTrue(version0.containsAll(solutionversion) && solutionversion.containsAll(version0));

		ArrayList<String> version1 = new ArrayList<String>();
		version1.add("true");
		solutionversion = solutionFirstLine.get(1);
		assertTrue(version1.containsAll(solutionversion) && solutionversion.containsAll(version1));

		ArrayList<String> version2 = new ArrayList<String>();
		version2.add("true");
		solutionversion = solutionFirstLine.get(2);
		assertTrue(version2.containsAll(solutionversion) && solutionversion.containsAll(version2));

		ArrayList<String> version3 = new ArrayList<String>();
		version3.add("false");
	
		solutionversion = solutionFirstLine.get(3);
		assertTrue(version3.containsAll(solutionversion) && solutionversion.containsAll(version3));
	}

	@Test
	public void testSimpleDiffwithDels() throws InterruptedException, ExecutionException {
		String query = QueryUtils.createLookupQueryGraph("subject", "<http://example.org/uri1>");
		DiffSolution solution = jenaArchive.diffQuerying(0, 1, query);

		ArrayList<String> solutionDel = solution.getDels();
		// //System.out.println(solutionDel);

		ArrayList<String> expectedDel = new ArrayList<String>();

		expectedDel.add("<http://example.org/predicate1> literal4");

		assertTrue(expectedDel.containsAll(solutionDel) && solutionDel.containsAll(expectedDel));
	}

	@Test
	public void testSimpleDiffwithAddsAndDeletes() throws InterruptedException, ExecutionException {
		String query = QueryUtils.createLookupQueryGraph("subject", "<http://example.org/uri4>");
		DiffSolution solution = jenaArchive.diffQuerying(2, 3, query);
		ArrayList<String> solutionAdd = solution.getAdds();
		// System.out.println(solutionAdd);
		ArrayList<String> solutionDel = solution.getDels();
		// System.out.println(solutionDel);

		ArrayList<String> expectedDel = new ArrayList<String>();
		ArrayList<String> expectedAdd = new ArrayList<String>();

		expectedAdd.add("<http://example.org/predicate4> <http://example.org/uri6>");
		expectedDel.add("<http://example.org/predicate4> <http://example.org/uri5>");

		assertTrue(expectedAdd.containsAll(solutionAdd) && solutionAdd.containsAll(expectedAdd));
		assertTrue(expectedDel.containsAll(solutionDel) && solutionDel.containsAll(expectedDel));
	}

	@Test
	public void testbulkAlldiffQueryingwithAddsAndDeletes() throws InterruptedException, ExecutionException, IOException {
		URL queryurl = this.getClass().getResource(FileSystems.getDefault().getSeparator() + "testCBTB/testQueryDynamic.txt");

		ArrayList<Map<Integer, DiffSolution>> solution = jenaArchive.bulkAlldiffQuerying(queryurl.getFile(), "subject", 1);

		//System.out.println(solution);
		Map<Integer, DiffSolution> solutionFirstLine = solution.get(0);
		/*
		 * diff 0 and 1
		 */
		ArrayList<String> solutionAdd = solutionFirstLine.get(1).getAdds();

		ArrayList<String> solutionDel = solutionFirstLine.get(1).getDels();

		ArrayList<String> expectedDel = new ArrayList<String>();
		ArrayList<String> expectedAdd = new ArrayList<String>();

		expectedDel.add("<http://example.org/predicate1> literal4");
		// System.out.println(solutionDel);
		assertTrue(expectedAdd.containsAll(solutionAdd) && solutionAdd.containsAll(expectedAdd));
		assertTrue(expectedDel.containsAll(solutionDel) && solutionDel.containsAll(expectedDel));

		/*
		 * diff 0 and 2
		 */
		solutionAdd = solutionFirstLine.get(2).getAdds();
		solutionDel = solutionFirstLine.get(2).getDels();

		expectedDel = new ArrayList<String>();
		expectedAdd = new ArrayList<String>();

		expectedDel.add("<http://example.org/predicate1> literal4");
		// System.out.println(solutionDel);
		assertTrue(expectedAdd.containsAll(solutionAdd) && solutionAdd.containsAll(expectedAdd));
		assertTrue(expectedDel.containsAll(solutionDel) && solutionDel.containsAll(expectedDel));

		/*
		 * diff 0 and 3
		 */
		solutionAdd = solutionFirstLine.get(3).getAdds();
		// System.out.println(solutionAdd);
		solutionDel = solutionFirstLine.get(3).getDels();
		// System.out.println(solutionDel);

		expectedDel = new ArrayList<String>();
		expectedAdd = new ArrayList<String>();

		expectedDel.add("<http://example.org/predicate1> literal1");
		expectedDel.add("<http://example.org/predicate1> literal2");
		expectedDel.add("<http://example.org/predicate1> literal3");
		assertTrue(expectedAdd.containsAll(solutionAdd) && solutionAdd.containsAll(expectedAdd));
		assertTrue(expectedDel.containsAll(solutionDel) && solutionDel.containsAll(expectedDel));

	}

	@Test
	public void testVerQuery() throws InterruptedException, ExecutionException, FileNotFoundException, IOException {

		String query = QueryUtils.createLookupQueryGraph("subject", "<http://example.org/uri1>");
		Map<Integer, ArrayList<String>> solution = jenaArchive.verQuery(query);

		ArrayList<String> version0 = new ArrayList<String>();
		version0.add("<http://example.org/predicate1> literal1");
		version0.add("<http://example.org/predicate1> literal2");
		version0.add("<http://example.org/predicate1> literal3");
		version0.add("<http://example.org/predicate1> literal4");
		version0.add("<http://example.org/predicate2> <http://example.org/uri2>");
		version0.add("<http://example.org/predicate2> <http://example.org/uri5>");
		//System.out.println(solution);
		ArrayList<String> solutionversion = solution.get(0);

		assertTrue(version0.containsAll(solutionversion) && solutionversion.containsAll(version0));

		ArrayList<String> version1 = new ArrayList<String>();
		version1.add("<http://example.org/predicate1> literal1");
		version1.add("<http://example.org/predicate1> literal2");
		version1.add("<http://example.org/predicate1> literal3");
		version1.add("<http://example.org/predicate2> <http://example.org/uri2>");
		version1.add("<http://example.org/predicate2> <http://example.org/uri5>");
		solutionversion = solution.get(1);
		assertTrue(version1.containsAll(solutionversion) && solutionversion.containsAll(version1));

		ArrayList<String> version2 = new ArrayList<String>();
		version2.add("<http://example.org/predicate1> literal1");
		version2.add("<http://example.org/predicate1> literal2");
		version2.add("<http://example.org/predicate1> literal3");
		version2.add("<http://example.org/predicate2> <http://example.org/uri2>");
		version2.add("<http://example.org/predicate2> <http://example.org/uri5>");
		solutionversion = solution.get(2);
		assertTrue(version2.containsAll(solutionversion) && solutionversion.containsAll(version2));

		ArrayList<String> version3 = new ArrayList<String>();
		version3.add("<http://example.org/predicate1> literal4");
		version3.add("<http://example.org/predicate2> <http://example.org/uri2>");
		version3.add("<http://example.org/predicate2> <http://example.org/uri5>");
		solutionversion = solution.get(3);
		assertTrue(version3.containsAll(solutionversion) && solutionversion.containsAll(version3));
	}

	@Test
	public void testBulkAllVerQuerying() throws InterruptedException, ExecutionException, FileNotFoundException, IOException {

		URL queryurl = this.getClass().getResource(FileSystems.getDefault().getSeparator() + "testCBTB/testQueryDynamic.txt");

		ArrayList<Map<Integer, ArrayList<String>>> solution = jenaArchive.bulkAllVerQuerying(queryurl.getFile(), "subject");
		//System.out.println(solution);
		Map<Integer, ArrayList<String>> solutionFirstLine = solution.get(0);

		ArrayList<String> version0 = new ArrayList<String>();
		version0.add("<http://example.org/predicate1> literal1");
		version0.add("<http://example.org/predicate1> literal2");
		version0.add("<http://example.org/predicate1> literal3");
		version0.add("<http://example.org/predicate1> literal4");
		version0.add("<http://example.org/predicate2> <http://example.org/uri2>");
		version0.add("<http://example.org/predicate2> <http://example.org/uri5>");
		ArrayList<String> solutionversion = solutionFirstLine.get(0);
		assertTrue(version0.containsAll(solutionversion) && solutionversion.containsAll(version0));

		ArrayList<String> version1 = new ArrayList<String>();
		version1.add("<http://example.org/predicate1> literal1");
		version1.add("<http://example.org/predicate1> literal2");
		version1.add("<http://example.org/predicate1> literal3");
		version1.add("<http://example.org/predicate2> <http://example.org/uri2>");
		version1.add("<http://example.org/predicate2> <http://example.org/uri5>");
		solutionversion = solutionFirstLine.get(1);
		assertTrue(version1.containsAll(solutionversion) && solutionversion.containsAll(version1));

		ArrayList<String> version2 = new ArrayList<String>();
		version2.add("<http://example.org/predicate1> literal1");
		version2.add("<http://example.org/predicate1> literal2");
		version2.add("<http://example.org/predicate1> literal3");
		version2.add("<http://example.org/predicate2> <http://example.org/uri2>");
		version2.add("<http://example.org/predicate2> <http://example.org/uri5>");
		solutionversion = solutionFirstLine.get(2);
		assertTrue(version2.containsAll(solutionversion) && solutionversion.containsAll(version2));

		ArrayList<String> version3 = new ArrayList<String>();
		version3.add("<http://example.org/predicate1> literal4");
		version3.add("<http://example.org/predicate2> <http://example.org/uri2>");
		version3.add("<http://example.org/predicate2> <http://example.org/uri5>");
		solutionversion = solutionFirstLine.get(3);
		assertTrue(version3.containsAll(solutionversion) && solutionversion.containsAll(version3));
	}

}
