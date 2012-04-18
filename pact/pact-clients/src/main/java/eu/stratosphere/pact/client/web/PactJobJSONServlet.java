/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.client.web;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import eu.stratosphere.pact.client.nephele.api.PactProgram;
import eu.stratosphere.pact.compiler.jobgen.JSONGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;

/**
 * @author Stephan Ewen (stephan.ewen@tu-berlin.com)
 */
public class PactJobJSONServlet extends HttpServlet {
	/**
	 * Serial UID for serialization interoperability.
	 */
	private static final long serialVersionUID = 558077298726449201L;

	// ------------------------------------------------------------------------

	private static final String JOB_PARAM_NAME = "job";

	// ------------------------------------------------------------------------

	private final File jobStoreDirectory; // the directory in which the jobs are stored

	public PactJobJSONServlet(File jobStoreDirectory) {
		this.jobStoreDirectory = jobStoreDirectory;
	}

	/*
	 * (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest,
	 * javax.servlet.http.HttpServletResponse)
	 */
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		resp.setContentType("application/json");

		String jobName = req.getParameter(JOB_PARAM_NAME);
		if (jobName == null) {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		// check, if the jar exists
		File jarFile = new File(jobStoreDirectory, jobName);
		if (!jarFile.exists()) {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		// create the pact plan
		PactProgram pactProgram;
		try {
			pactProgram = new PactProgram(jarFile, new String[0]);
		}
		catch (Throwable t) {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			resp.getWriter().print(t.getMessage());
			return;
		}
		
		String jsonPlan = null;
		String programDescription = null;
		
		try {
			OptimizedPlan optPlan = pactProgram.getPreviewPlan();
			jsonPlan = new JSONGenerator().compilePlanToJSON(optPlan);
		}
		catch (Throwable t) {}
		
		try {
			programDescription = pactProgram.getDescription();
		}
		catch (Throwable t) {}
			
		if (jsonPlan == null && programDescription == null) {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return;
		} else {
			resp.setStatus(HttpServletResponse.SC_OK);
			PrintWriter wrt = resp.getWriter();
			wrt.print("{ \"jobname\": \"");
			wrt.print(jobName);
			if (jsonPlan != null) {
				wrt.print("\", \"plan\": ");
				wrt.println(jsonPlan);
			}
			if (programDescription != null) {
				wrt.print(", \"description\": \"");
				wrt.print(escapeString(programDescription));
			}
			
			wrt.print("\"");
			wrt.println("}");
		}
	}

	protected String escapeString(String str) {
		int len = str.length();
		char[] s = str.toCharArray();
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < len; i += 1) {
			char c = s[i];
			if ((c == '\\') || (c == '"') || (c == '/')) {
				sb.append('\\');
				sb.append(c);
			}
			else if (c == '\b')
				sb.append("\\b");
			else if (c == '\t')
				sb.append("\\t");
			else if (c == '\n')
				sb.append("<br>");
			else if (c == '\f')
				sb.append("\\f");
			else if (c == '\r')
				sb.append("\\r");
			else if (c == '>')
				sb.append("&gt;");
			else if (c == '<')
				sb.append("&lt;");
			else {
				if (c < ' ') {
					// Unreadable throw away
				} else {
					sb.append(c);
				}
			}
		}

		return sb.toString();
	}
}
