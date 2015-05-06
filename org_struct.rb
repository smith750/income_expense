require 'kfsdb'

class Org
	attr_reader :chart, :org, :name, :type_name, :number_of_accounts
	
	def initialize(chart, org, name, type_name)
		@chart = chart
		@org = org
		@name = name
		@type_name = type_name
		@number_of_accounts = 0
	end
	
	def populate_children!(con)
		#puts "populating children for #{@chart} #{@org}"
		@children = []
		org_type_code = ""
		con.query("select ca_org_t.fin_coa_cd, ca_org_t.org_cd, ca_org_t.org_nm, ca_org_type_t.org_typ_nm from ca_org_t join ca_org_type_t on ca_org_t.org_typ_cd = ca_org_type_t.org_typ_cd where ca_org_t.rpts_to_fin_coa_cd = ? and ca_org_t.rpts_to_org_cd = ?", @chart, @org) do |row|
			#puts "retrieved "+row["fin_coa_cd"]+" "+row["org_cd"]+" by #{@chart} #{@org}"
			if row["fin_coa_cd"] != @chart || row["org_cd"] != @org
				#puts "so i'll add it"
				@children << Org.new(row["fin_coa_cd"], row["org_cd"], row["org_nm"], row["org_typ_nm"])
			end
		end
		
		con.query("select count(*) as c from ca_account_t where fin_coa_cd = ? and org_cd = ?",@chart,@org) do |row|
			@number_of_accounts = row["c"]
		end
		#puts "children size: #{@children.size}"
		@children.each { |kid_org| kid_org.populate_children!(con) }
	end
	
	def to_s
		"<ul><li>#{@chart}-#{@org} #{@name} TYPE: #{@type_name} (number of accounts: #{@number_of_accounts})#{@children.collect{|kid_org| kid_org.to_s}.join("")}</li></ul>"
	end
end

def build_dept(con)
	org = Org.new("IU", "UNIV", "UNIVERSITY LEVEL", "UNIVERSITY")
	org.populate_children!(con)
	return org.to_s
end

s = ""
db_connect("kfstst_at_kfsstg") do |con|
	s << "<html><head><title>KFS Demo Data Org Structure</title><link rel=\"stylesheet\" href=\"http://static.jstree.com/3.1.1/assets/bootstrap/css/bootstrap.min.css\" /><link rel=\"stylesheet\" href=\"http://static.jstree.com/3.1.1/assets/dist/themes/default/style.min.css\" />"
	s << "<head><body>"
	s << "<script type=\"text/javascript\" src=\"https://code.jquery.com/jquery-1.10.2.js\"></script>\n<script src=\"http://code.jquery.com/jquery.address-1.6.js\"></script>\n<script src=\"http://static.jstree.com/3.1.1/assets/vakata.js\"></script>\n<script type=\"text/javascript\" src=\"https://cdnjs.cloudflare.com/ajax/libs/jstree/3.1.0/jstree.min.js\"></script>\n<script type=\"text/javascript\">\n\t\t$(document).ready(function() {\n\t\t\t$('#org-struct').jstree({\"core\" : {\"themes\" : { \"variant\" : \"large\" }}});\n\t\t});\n</script>"		
	s << "<div id=\"org-struct\">"
	s << build_dept(con)
	s << "</div></body></html>"
end

puts s