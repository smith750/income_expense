require 'kfsdb'

class Org
	attr_reader :chart, :org, :name, :children
	
	def initialize(chart, org, name)
		@chart = chart
		@org = org
		@name = name
	end
	
	def populate_children!(con)
		#puts "populating children for #{@chart} #{@org}"
		@children = []
		con.query("select fin_coa_cd, org_cd, org_nm from ca_org_t where RPTS_TO_FIN_COA_CD = '#{@chart}' and RPTS_TO_ORG_CD = '#{@org}'") do |row|
			#puts "retrieved "+row["fin_coa_cd"]+" "+row["org_cd"]+" by #{@chart} #{@org}"
			if row["fin_coa_cd"] != @chart || row["org_cd"] != @org
				#puts "so i'll add it"
				@children << Org.new(row["fin_coa_cd"], row["org_cd"], row["org_nm"])
			end
		end
		#puts "children size: #{@children.size}"
		@children.each { |kid_org| kid_org.populate_children!(con) }
	end
	
	def to_s
		"<ul><li>#{@chart}-#{@org} #{@name}#{@children.collect{|kid_org| kid_org.to_s}.join("")}</li></ul>"
	end
end

def build_dept(con)
	org = Org.new("IU", "UNIV", "UNIVERSITY LEVEL")
	org.populate_children!(con)
	return org.to_s
end

s = ""
db_connect("kfstst_at_kfsstg") do |con|
	s << "<html><head><title>KFS Demo Data Org Structure</title><head><body>"
	s << build_dept(con)
	s << "</body></html>"
end

puts s