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


def lookup_campus_and_college(fin_coa_cd, org_cd, con)
end

def lookup_basic_balance_info(con)
	query =<<-QUERY
select distinct
  gl_balance_t.UNIV_FISCAL_YR, gl_balance_t.FIN_COA_CD, gl_balance_t.ACCOUNT_NBR, gl_balance_t.SUB_ACCT_NBR, ca_org_t.fin_coa_cd as org_fin_coa_cd, ca_org_t.org_cd, ca_org_t.org_nm, gl_balance_t.fin_obj_typ_cd,
  CA_OBJ_CONSOLDTN_T.FIN_COA_CD as cons_fin_coa_cd, CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_CD, CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_NM, 
  ca_account_t.ACCT_FSC_OFC_UID, ca_account_t.ACCT_SPVSR_UNVL_ID, ca_account_t.ACCT_MGR_UNVL_ID, ca_org_t.ORG_MGR_UNVL_ID, ca_account_t.ACCT_CLOSED_IND,
  ca_obj_type_t.fin_obj_typ_nm, ca_acctg_ctgry_t.acctg_ctgry_desc, ca_sub_acct_t.sub_acct_nm,
  ca_sub_fund_grp_t.sub_fund_grp_cd, ca_fund_grp_t.fund_grp_cd, ca_fund_grp_t.fund_grp_nm
from (ca_org_t 
        join (ca_account_t 
          join (CA_SUB_FUND_GRP_T
              join CA_FUND_GRP_T
              on CA_SUB_FUND_GRP_T.FUND_GRP_CD = CA_FUND_GRP_T.FUND_GRP_CD)
          on ca_account_t.SUB_FUND_GRP_CD = CA_SUB_FUND_GRP_T.SUB_FUND_GRP_CD)
        on ca_account_t.fin_coa_cd = ca_org_t.fin_coa_cd and ca_account_t.org_cd = ca_org_t.org_cd)
  join (((gl_balance_t
          join 
            (ca_object_code_t
                join (CA_OBJ_LEVEL_T
                  join CA_OBJ_CONSOLDTN_T
                  on CA_OBJ_LEVEL_T.FIN_COA_CD = CA_OBJ_CONSOLDTN_T.FIN_COA_CD and CA_OBJ_LEVEL_T.FIN_CONS_OBJ_CD = CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_CD)
                on ca_object_code_t.FIN_OBJ_LEVEL_CD = CA_OBJ_LEVEL_T.fin_obj_level_cd and ca_object_code_t.fin_coa_cd = ca_obj_level_t.fin_coa_cd)
          on gl_balance_t.univ_fiscal_yr = ca_object_code_t.univ_fiscal_yr and gl_balance_t.fin_coa_cd = ca_object_code_t.fin_coa_cd and gl_balance_t.fin_object_cd = ca_object_code_t.fin_object_cd)
          left join ca_sub_acct_t
          on gl_balance_t.fin_coa_cd = ca_sub_acct_t.fin_coa_cd and gl_balance_t.account_nbr = ca_sub_acct_t.account_nbr and gl_balance_t.sub_acct_nbr = ca_sub_acct_t.sub_acct_nbr)
		  join (ca_obj_type_t
              join ca_acctg_ctgry_t
              on ca_obj_type_t.acctg_ctgry_cd = ca_acctg_ctgry_t.acctg_ctgry_cd)
      on gl_balance_t.fin_obj_typ_cd = ca_obj_type_t.fin_obj_typ_cd)
   on ca_account_t.fin_coa_cd = gl_balance_t.fin_coa_cd and ca_account_t.account_nbr = gl_balance_t.account_nbr
where gl_balance_t.univ_fiscal_yr = 2015 and ca_acctg_ctgry_t.acctg_ctgry_cd in ('IN','EX')
QUERY

	recs = []
	con.query(query) do |row|
		column_names = row.columns.collect{|col| col.name}
		rec = row.to_hash
		recs << rec
	end
	
	recs
end

def update_principals(rec, principal_finder, rice_con)
	rec["fiscal_officer_principal_name"] = principal_finder.find_principal_name(rec.delete("acct_fsc_ofc_uid"),rice_con)
	rec["account_supervisor_principal_name"] = principal_finder.find_principal_name(rec.delete("acct_spvsr_unvl_id"),rice_con)
	rec["account_manager_principal_name"] = principal_finder.find_principal_name(rec.delete("acct_mgr_unvl_id"),rice_con)
	rec["organization_manager_principal_name"] = principal_finder.find_principal_name(rec.delete("org_mgr_unvl_id"),rice_con)
end

def clean_amount(balance_info, amount_key)
	if balance_info[amount_key].nil?
		balance_info[amount_key] = 0.0 
	else
		balance_info[amount_key] = balance_info[amount_key].to_f
	end
end

def update_by_base_balance(balance_info, con)
	bb_query =<<-BBQUERY
select sum(gl_balance_t.FIN_BEG_BAL_LN_AMT) as original_budget, sum(gl_balance_t.ACLN_ANNL_BAL_AMT) as base_budget
	from gl_balance_t join
		(ca_object_code_t
                join (CA_OBJ_LEVEL_T
                      join CA_OBJ_CONSOLDTN_T
                      on CA_OBJ_LEVEL_T.FIN_COA_CD = CA_OBJ_CONSOLDTN_T.FIN_COA_CD and CA_OBJ_LEVEL_T.FIN_CONS_OBJ_CD = CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_CD)
                on ca_object_code_t.FIN_OBJ_LEVEL_CD = CA_OBJ_LEVEL_T.fin_obj_level_cd and ca_object_code_t.fin_coa_cd = ca_obj_level_t.fin_coa_cd)
    on gl_balance_t.univ_fiscal_yr = ca_object_code_t.univ_fiscal_yr and gl_balance_t.fin_coa_cd = ca_object_code_t.fin_coa_cd and gl_balance_t.fin_object_cd = ca_object_code_t.fin_object_cd
	where gl_balance_t.univ_fiscal_yr = ? and gl_balance_t.fin_coa_cd = ? and gl_balance_t.account_nbr = ? and gl_balance_t.sub_acct_nbr = ? and ca_obj_consoldtn_t.fin_cons_obj_cd = ? and gl_balance_t.fin_balance_typ_cd = 'BB'
BBQUERY
	con.query(bb_query, balance_info["univ_fiscal_yr"], balance_info["fin_coa_cd"], balance_info["account_nbr"], balance_info["sub_acct_nbr"], balance_info["fin_cons_obj_cd"]) do |row|
		balance_info["original_budget"] = row["original_budget"]
		balance_info["base_budget"] = row["base_budget"]
	end
	clean_amount(balance_info, "original_budget")
	clean_amount(balance_info, "base_budget")
end

def update_by_current_balance(balance_info, con)
	cb_query = <<-CBQUERY
select sum(gl_balance_t.ACLN_ANNL_BAL_AMT) as current_budget
	from gl_balance_t join
		(ca_object_code_t
                join (CA_OBJ_LEVEL_T
                      join CA_OBJ_CONSOLDTN_T
                      on CA_OBJ_LEVEL_T.FIN_COA_CD = CA_OBJ_CONSOLDTN_T.FIN_COA_CD and CA_OBJ_LEVEL_T.FIN_CONS_OBJ_CD = CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_CD)
                on ca_object_code_t.FIN_OBJ_LEVEL_CD = CA_OBJ_LEVEL_T.fin_obj_level_cd and ca_object_code_t.fin_coa_cd = ca_obj_level_t.fin_coa_cd)
    on gl_balance_t.univ_fiscal_yr = ca_object_code_t.univ_fiscal_yr and gl_balance_t.fin_coa_cd = ca_object_code_t.fin_coa_cd and gl_balance_t.fin_object_cd = ca_object_code_t.fin_object_cd
	where gl_balance_t.univ_fiscal_yr = ? and gl_balance_t.fin_coa_cd = ? and gl_balance_t.account_nbr = ? and gl_balance_t.sub_acct_nbr = ? and ca_obj_consoldtn_t.fin_cons_obj_cd = ? and gl_balance_t.fin_balance_typ_cd = 'CB'
CBQUERY
	con.query(cb_query, balance_info["univ_fiscal_yr"], balance_info["fin_coa_cd"], balance_info["account_nbr"], balance_info["sub_acct_nbr"], balance_info["fin_cons_obj_cd"]) do |row|
		balance_info["current_budget"] = row["current_budget"]
	end
	clean_amount(balance_info, "current_budget")
end

def update_by_open_encumbrance(balance_info, con)
	en_query = <<-ENQUERY
select sum(gl_balance_t.ACLN_ANNL_BAL_AMT) as open_encumbrances
	from gl_balance_t join
		(ca_object_code_t
                join (CA_OBJ_LEVEL_T
                      join CA_OBJ_CONSOLDTN_T
                      on CA_OBJ_LEVEL_T.FIN_COA_CD = CA_OBJ_CONSOLDTN_T.FIN_COA_CD and CA_OBJ_LEVEL_T.FIN_CONS_OBJ_CD = CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_CD)
                on ca_object_code_t.FIN_OBJ_LEVEL_CD = CA_OBJ_LEVEL_T.fin_obj_level_cd and ca_object_code_t.fin_coa_cd = ca_obj_level_t.fin_coa_cd)
    on gl_balance_t.univ_fiscal_yr = ca_object_code_t.univ_fiscal_yr and gl_balance_t.fin_coa_cd = ca_object_code_t.fin_coa_cd and gl_balance_t.fin_object_cd = ca_object_code_t.fin_object_cd
	where gl_balance_t.univ_fiscal_yr = ? and gl_balance_t.fin_coa_cd = ? and gl_balance_t.account_nbr = ? and gl_balance_t.sub_acct_nbr = ? and ca_obj_consoldtn_t.fin_cons_obj_cd = ? and gl_balance_t.fin_balance_typ_cd in ('EX','IE','CE')
ENQUERY
	con.query(en_query, balance_info["univ_fiscal_yr"], balance_info["fin_coa_cd"], balance_info["account_nbr"], balance_info["sub_acct_nbr"], balance_info["fin_cons_obj_cd"]) do |row|
		balance_info["open_encumbrances"] = row["open_encumbrances"]
	end
	clean_amount(balance_info, "open_encumbrances")
end

def update_by_pre_encumbrance(balance_info, con)
	pe_query = <<-PEQUERY
select sum(gl_balance_t.ACLN_ANNL_BAL_AMT) as pre_encumbrance
	from gl_balance_t join
		(ca_object_code_t
                join (CA_OBJ_LEVEL_T
                      join CA_OBJ_CONSOLDTN_T
                      on CA_OBJ_LEVEL_T.FIN_COA_CD = CA_OBJ_CONSOLDTN_T.FIN_COA_CD and CA_OBJ_LEVEL_T.FIN_CONS_OBJ_CD = CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_CD)
                on ca_object_code_t.FIN_OBJ_LEVEL_CD = CA_OBJ_LEVEL_T.fin_obj_level_cd and ca_object_code_t.fin_coa_cd = ca_obj_level_t.fin_coa_cd)
    on gl_balance_t.univ_fiscal_yr = ca_object_code_t.univ_fiscal_yr and gl_balance_t.fin_coa_cd = ca_object_code_t.fin_coa_cd and gl_balance_t.fin_object_cd = ca_object_code_t.fin_object_cd
	where gl_balance_t.univ_fiscal_yr = ? and gl_balance_t.fin_coa_cd = ? and gl_balance_t.account_nbr = ? and gl_balance_t.sub_acct_nbr = ? and ca_obj_consoldtn_t.fin_cons_obj_cd = ? and gl_balance_t.fin_balance_typ_cd = 'PE'
PEQUERY
	con.query(pe_query, balance_info["univ_fiscal_yr"], balance_info["fin_coa_cd"], balance_info["account_nbr"], balance_info["sub_acct_nbr"], balance_info["fin_cons_obj_cd"]) do |row|
		balance_info["pre_encumbrance"] = row["pre_encumbrance"]
	end
	clean_amount(balance_info, "pre_encumbrance")
end

def update_by_actual(balance_info, con)
	ac_query = <<-ACQUERY
select sum(CONTR_GR_BB_AC_AMT + ACLN_ANNL_BAL_AMT) as inception_to_date
	from gl_balance_t join
		(ca_object_code_t
                join (CA_OBJ_LEVEL_T
                      join CA_OBJ_CONSOLDTN_T
                      on CA_OBJ_LEVEL_T.FIN_COA_CD = CA_OBJ_CONSOLDTN_T.FIN_COA_CD and CA_OBJ_LEVEL_T.FIN_CONS_OBJ_CD = CA_OBJ_CONSOLDTN_T.FIN_CONS_OBJ_CD)
                on ca_object_code_t.FIN_OBJ_LEVEL_CD = CA_OBJ_LEVEL_T.fin_obj_level_cd and ca_object_code_t.fin_coa_cd = ca_obj_level_t.fin_coa_cd)
    on gl_balance_t.univ_fiscal_yr = ca_object_code_t.univ_fiscal_yr and gl_balance_t.fin_coa_cd = ca_object_code_t.fin_coa_cd and gl_balance_t.fin_object_cd = ca_object_code_t.fin_object_cd
	where gl_balance_t.univ_fiscal_yr = ? and gl_balance_t.fin_coa_cd = ? and gl_balance_t.account_nbr = ? and gl_balance_t.sub_acct_nbr = ? and ca_obj_consoldtn_t.fin_cons_obj_cd = ? and gl_balance_t.fin_balance_typ_cd = 'AC'
ACQUERY
	balance_info["inception_to_date"] = 0.0
	con.query(ac_query, balance_info["univ_fiscal_yr"], balance_info["fin_coa_cd"], balance_info["account_nbr"], balance_info["sub_acct_nbr"], balance_info["fin_cons_obj_cd"]) do |row|
		balance_info["inception_to_date"] = row["inception_to_date"]
	end
	clean_amount(balance_info, "inception_to_date")
	balance_info["balance_available"] = balance_info["current_budget"] - balance_info["inception_to_date"] - balance_info["open_encumbrances"] - balance_info["pre_encumbrance"]
	clean_amount(balance_info, "balance_available")
end

balance_infos = []
db_connect("kfstst_at_kfsstg") do |con|
	balance_infos = lookup_basic_balance_info(con)
	
	principal_finder = PrincipalFinder.new()
	db_connect("ricetst_at_kfsstg") do |rice_con|
		balance_infos.each {|balance_info| update_principals(balance_info, principal_finder, rice_con) }
	end
	balance_infos.each do |balance_info| 
		update_by_base_balance(balance_info, con)
		update_by_current_balance(balance_info, con)
		update_by_open_encumbrance(balance_info, con)
		update_by_pre_encumbrance(balance_info, con)
		update_by_actual(balance_info, con)
	end
end

File.open("balances_query.json","w") do |fout|
	balance_infos.each do |rec|
		fout.write(JSON.generate(rec)+"\n")
	end
end

col_names = balance_infos[0].keys
CSV.open("balances_query.csv","wb") do |csv|
	csv << col_names
	balance_infos.each do |rec|
		values = col_names.inject([]) {|memo, value| memo << rec[value]}
		csv << values
	end
end