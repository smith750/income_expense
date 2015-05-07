require 'kfsdb'
require 'csv'
require 'json'

Org = Struct.new(:chart, :org, :name, :type)
OrgKey = Struct.new(:chart, :org)

class OrgFinder
	def initialize
		@parents = {}
		@orgs = {}
	end
	
	def find_campus(chart, org, con)
	end
	
	def find_school(chart, org, con)
	end
	
	private
	
	def find_parent(chart, org, con)
		key = OrgKey.new(chart, org)
		if @parents.include?(key)
			@parents[key]
		else
			org = nil
			con.query("select fin_coa_cd, org_cd, org_nm, org_typ_cd from ca_org_t where RPTS_TO_FIN_COA_CD = ? and RPTS_TO_ORG_CD = ?", chart, org) do |row|
				org = Org.new(row["fin_coa_cd"], row["org_cd"], row["org_nm"], row["org_typ_cd"])
				@parents[key] = org
			end
			org
		end
	end
	
	def find_org(chart, org, con)
		key = OrgKey.new(chart, org)
		if @orgs.include?(key)
			@orgs[key]
		else
			org = nil
			con.query("select org_nm, org_typ_cd from ca_org_t where fin_coa_cd = ? and org_cd = ?", chart, org) do |row|
				org = Org.new(chart, org, row["org_nm"], row["org_typ_cd"])
				@orgs[key] = org
			end
			org
		end
	end
end

class PrincipalFinder
	def initialize
		@principals = {}
	end
	
	def find_principal_name(principal_id, con)
		if @principals.include?(principal_id)
			@principals[principal_id]
		else
			principal_name = ""
			con.query("select prncpl_nm from krim_prncpl_t where prncpl_id = ?",principal_id) do |row|
				principal_name = row["prncpl_nm"]
				@principals[principal_id] = principal_name
			end
			principal_name
		end
	end
end


query =<<-QUERY
select gl_balance_t.UNIV_FISCAL_YR, gl_balance_t.FIN_COA_CD, gl_balance_t.ACCOUNT_NBR, gl_balance_t.SUB_ACCT_NBR, gl_balance_t.FIN_OBJECT_CD, gl_balance_t.FIN_SUB_OBJ_CD, gl_balance_t.fin_balance_typ_cd,
	gl_balance_t.ACLN_ANNL_BAL_AMT, gl_balance_t.FIN_BEG_BAL_LN_AMT, gl_balance_t.CONTR_GR_BB_AC_AMT, gl_balance_t.MO1_ACCT_LN_AMT, gl_balance_t.MO2_ACCT_LN_AMT, gl_balance_t.MO3_ACCT_LN_AMT, gl_balance_t.MO4_ACCT_LN_AMT, gl_balance_t.MO5_ACCT_LN_AMT, 
	gl_balance_t.MO6_ACCT_LN_AMT, gl_balance_t.MO7_ACCT_LN_AMT, gl_balance_t.MO8_ACCT_LN_AMT, gl_balance_t.MO9_ACCT_LN_AMT, gl_balance_t.MO10_ACCT_LN_AMT, gl_balance_t.MO11_ACCT_LN_AMT, gl_balance_t.MO12_ACCT_LN_AMT, gl_balance_t.MO13_ACCT_LN_AMT,
	gl_balance_t.TIMESTAMP, ca_org_t.fin_coa_cd as org_fin_coa_cd, ca_org_t.org_cd, ca_org_t.org_nm, ca_object_code_t.fin_obj_cd_nm, 
	ca_object_code_t.FIN_OBJ_TYP_CD, CA_OBJ_LEVEL_T.fin_coa_cd as lvl_fin_coa_cd, ca_obj_level_t.fin_obj_level_cd, ca_obj_level_t.fin_obj_level_nm, 
	CA_OBJ_CONSOLDTN_T.FIN_COA_CD as cons_fin_coa_cd, CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_CD, CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_NM, 
	ca_account_t.ACCT_FSC_OFC_UID, ca_account_t.ACCT_SPVSR_UNVL_ID, ca_account_t.ACCT_MGR_UNVL_ID, ca_org_t.ORG_MGR_UNVL_ID, ca_account_t.ACCT_CLOSED_IND,
  ca_obj_type_t.fin_obj_typ_nm, ca_acctg_ctgry_t.acctg_ctgry_desc,
  ca_sub_fund_grp_t.sub_fund_grp_cd, ca_fund_grp_t.fund_grp_cd, ca_fund_grp_t.fund_grp_nm
from (ca_org_t 
        join (ca_account_t 
				join (CA_SUB_FUND_GRP_T
						join CA_FUND_GRP_T
						on CA_SUB_FUND_GRP_T.FUND_GRP_CD = CA_FUND_GRP_T.FUND_GRP_CD)
				on ca_account_t.SUB_FUND_GRP_CD = CA_SUB_FUND_GRP_T.SUB_FUND_GRP_CD)
        on ca_account_t.fin_coa_cd = ca_org_t.fin_coa_cd and ca_account_t.org_cd = ca_org_t.org_cd)
  join (gl_balance_t
          join 
            ((ca_object_code_t
              join (CA_OBJ_LEVEL_T
                join CA_OBJ_CONSOLDTN_T
                on CA_OBJ_LEVEL_T.FIN_COA_CD = CA_OBJ_CONSOLDTN_T.FIN_COA_CD and CA_OBJ_LEVEL_T.FIN_CONS_OBJ_CD = CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_CD)
              on ca_object_code_t.FIN_OBJ_LEVEL_CD = CA_OBJ_LEVEL_T.fin_obj_level_cd and ca_object_code_t.fin_coa_cd = ca_obj_level_t.fin_coa_cd)
              join ca_obj_type_t
                    join ca_acctg_ctgry_t
                    on ca_obj_type_t.acctg_ctgry_cd = ca_acctg_ctgry_t.acctg_ctgry_cd
              on ca_object_code_t.fin_obj_typ_cd = ca_obj_type_t.fin_obj_typ_cd)
          on gl_balance_t.univ_fiscal_yr = ca_object_code_t.univ_fiscal_yr and gl_balance_t.fin_coa_cd = ca_object_code_t.fin_coa_cd and gl_balance_t.fin_object_cd = ca_object_code_t.fin_object_cd)
   on ca_account_t.fin_coa_cd = gl_balance_t.fin_coa_cd and ca_account_t.account_nbr = gl_balance_t.account_nbr
where gl_balance_t.univ_fiscal_yr = 2015 and ca_acctg_ctgry_t.acctg_ctgry_cd in ('IN','EX')
QUERY

def lookup_campus_and_college(fin_coa_cd, org_cd, con)
end

removed_columns = ["acct_fsc_ofc_uid", "acct_spvsr_unvl_id", "acct_mgr_unvl_id", "org_mgr_unvl_id"]
added_columns = ["fiscal_officer_principal_name","account_supervisor_principal_name","account_manager_principal_name","organization_manager_principal_name"]

recs = []
column_names = []

principal_finder = PrincipalFinder.new()

db_connect("kfstst_at_kfsstg") do |con|
	db_connect("ricetst_at_kfsstg") do |rice_con|
		con.query(query) do |row|
			column_names = row.columns.collect{|col| col.name}
			rec = row.to_hash
			rec["fiscal_officer_principal_name"] = principal_finder.find_principal_name(rec.delete("acct_fsc_ofc_uid"),rice_con)
			rec["account_supervisor_principal_name"] = principal_finder.find_principal_name(rec.delete("acct_spvsr_unvl_id"),rice_con)
			rec["account_manager_principal_name"] = principal_finder.find_principal_name(rec.delete("acct_mgr_unvl_id"),rice_con)
			rec["organization_manager_principal_name"] = principal_finder.find_principal_name(rec.delete("org_mgr_unvl_id"),rice_con)
			recs << rec
		end
	end
	column_names = column_names.reject{|val| removed_columns.include?(val)} + added_columns
end

File.open("balances_query.json","w") do |fout|
	recs.each do |rec|
		fout.write(JSON.generate(rec)+"\n")
	end
end

CSV.open("balances_query.csv","wb") do |csv|
	csv << column_names
	recs.each do |rec|
		values = column_names.inject([]) {|memo, value| memo << rec[value]}
		csv << values
	end
end