package org.ai.wu.ac.at.rdfstarArchive.utils;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;

public final class QueryResult {
	QueryExecution ex;
	ResultSet sol;
	Boolean solAsk;
	public Boolean getSolAsk() {
		return solAsk;
	}
	public void setSolAsk(Boolean solAsk) {
		this.solAsk = solAsk;
	}
	int version;
	Boolean isAdd; // otherwise is del
	public Boolean getIsAdd() {
		return isAdd;
	}
	public void setIsAdd(Boolean isAdd) {
		this.isAdd = isAdd;
	}
	public QueryExecution getEx() {
		return ex;
	}
	public void setEx(QueryExecution ex) {
		this.ex = ex;
	}
	public ResultSet getSol() {
		return sol;
	}
	
	public void setSol(ResultSet sol) {
		this.sol = sol;
	}
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}
	
}