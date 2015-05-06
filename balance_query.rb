require 'kfsdb'
require 'csv'
require 'json'

OrgHolder = Struct.new(:chart, :org, :name, :type)

class OrgFinder
	def initialize
		@parents = {}
	end
	
	def find_campus(chart, org, con)
	end
	
	def find_school(chart, org, con)
	end
	
	private
	
	def find_parent(chart, org, con)
		
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
  ca_obj_type_t.fin_obj_typ_nm, ca_acctg_ctgry_t.acctg_ctgry_desc
from (ca_org_t 
        join ca_account_t 
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
where gl_balance_t.univ_fiscal_yr = 2015 
QUERY

def lookup_principal_name(principal_id, con)
	principal_name = nil
	con.query("select prncpl_nm from krim_prncpl_t where prncpl_id = ?",principal_id) do |row|
		principal_name = row["prncpl_nm"]
	end
	principal_name
end

def lookup_campus_and_college(fin_coa_cd, org_cd, con)
end

removed_columns = ["acct_fsc_ofc_uid", "acct_spvsr_unvl_id", "acct_mgr_unvl_id", "org_mgr_unvl_id"]
added_columns = ["fiscal_officer_principal_name","account_supervisor_principal_name","account_manager_principal_name","organization_manager_principal_name"]

recs = []
column_names = []
db_connect("kfstst_at_kfsstg") do |con|
	db_connect("ricetst_at_kfsstg") do |rice_con|
		con.query(query) do |row|
			column_names = row.columns.collect{|col| col.name}
			rec = row.to_hash
			rec["fiscal_officer_principal_name"] = lookup_principal_name(rec.delete("acct_fsc_ofc_uid"),rice_con)
			rec["account_supervisor_principal_name"] = lookup_principal_name(rec.delete("acct_spvsr_unvl_id"),rice_con)
			rec["account_manager_principal_name"] = lookup_principal_name(rec.delete("acct_mgr_unvl_id"),rice_con)
			rec["organization_manager_principal_name"] = lookup_principal_name(rec.delete("org_mgr_unvl_id"),rice_con)
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