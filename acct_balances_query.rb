require 'kfsdb'
require 'csv'
require 'json'

query =<<-QUERY
select gl_acct_balances_t.UNIV_FISCAL_YR, gl_acct_balances_t.FIN_COA_CD, gl_acct_balances_t.ACCOUNT_NBR, gl_acct_balances_t.SUB_ACCT_NBR, gl_acct_balances_t.FIN_OBJECT_CD, gl_acct_balances_t.FIN_SUB_OBJ_CD, gl_acct_balances_t.CURR_BDLN_BAL_AMT, 
	gl_acct_balances_t.ACLN_ACTLS_BAL_AMT, gl_acct_balances_t.ACLN_ENCUM_BAL_AMT, gl_acct_balances_t.TIMESTAMP, ca_org_t.fin_coa_cd as org_fin_coa_cd, ca_org_t.org_cd, ca_org_t.org_nm, ca_object_code_t.fin_obj_cd_nm, 
	ca_object_code_t.FIN_OBJ_TYP_CD, CA_OBJ_LEVEL_T.fin_coa_cd as lvl_fin_coa_cd, ca_obj_level_t.fin_obj_level_cd, ca_obj_level_t.fin_obj_level_nm, 
	CA_OBJ_CONSOLDTN_T.FIN_COA_CD as cons_fin_coa_cd, CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_CD, CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_NM, 
	ca_account_t.ACCT_FSC_OFC_UID, ca_account_t.ACCT_SPVSR_UNVL_ID, ca_account_t.ACCT_MGR_UNVL_ID, ca_org_t.ORG_MGR_UNVL_ID, ca_account_t.ACCT_CLOSED_IND
from (ca_org_t 
        join ca_account_t 
        on ca_account_t.fin_coa_cd = ca_org_t.fin_coa_cd and ca_account_t.org_cd = ca_org_t.org_cd)
  join (gl_acct_balances_t
          join (ca_object_code_t
                  join (CA_OBJ_LEVEL_T
                          join CA_OBJ_CONSOLDTN_T
                          on CA_OBJ_LEVEL_T.FIN_COA_CD = CA_OBJ_CONSOLDTN_T.FIN_COA_CD and CA_OBJ_LEVEL_T.FIN_CONS_OBJ_CD = CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_CD)
                  on ca_object_code_t.FIN_OBJ_LEVEL_CD = CA_OBJ_LEVEL_T.fin_obj_level_cd and ca_object_code_t.fin_coa_cd = ca_obj_level_t.fin_coa_cd)
          on gl_acct_balances_t.univ_fiscal_yr = ca_object_code_t.univ_fiscal_yr and gl_acct_balances_t.fin_coa_cd = ca_object_code_t.fin_coa_cd and gl_acct_balances_t.fin_object_cd = ca_object_code_t.fin_object_cd) 
  on ca_account_t.fin_coa_cd = gl_acct_balances_t.fin_coa_cd and ca_account_t.account_nbr = gl_acct_balances_t.account_nbr
  where gl_acct_balances_t.univ_fiscal_yr = 2015 
QUERY

recs = []
column_names = []
db_connect("kfstst_at_kfsstg") do |con|
	con.query(query) do |row|
		column_names = row.columns().collect{|col| col.name}
		rec = row.columns().inject({}) {|memo, value| memo[value.name] = row[value.name]; memo}
		recs << rec
	end
end

File.open("acct_balances_query.json","w") do |fout|
	recs.each do |rec|
		fout.write(JSON.generate(rec)+"\n")
	end
end

CSV.open("acct_balances_query.csv","wb") do |csv|
	csv << column_names
	recs.each do |rec|
		values = column_names.inject([]) {|memo, value| memo << rec[value]}
		csv << values
	end
end