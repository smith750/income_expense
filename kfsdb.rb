require 'java'
require 'yaml'
require 'jruby'

java_import "java.sql.Types"

class Column
	attr_reader :name, :position, :type, :precision, :scale
	
	def initialize(meta_data, position)
		@name = meta_data.getColumnName(position).downcase
		@position = position
		jdbc_type = meta_data.getColumnType(position)
		#puts "initing column #{@name} jdbc type #{jdbc_type}"
		if jdbc_type == java.sql.Types::DECIMAL || jdbc_type == java.sql.Types::FLOAT || jdbc_type == java.sql.Types::DOUBLE || jdbc_type == java.sql.Types::REAL || jdbc_type == java.sql.Types::NUMERIC
			@scale = meta_data.getScale(position)
			@precision = meta_data.getPrecision(position)
			if @scale == 0
				jdbc_type = java.sql.Types::INTEGER
			end
		end
		@type = jdbc_type_to_type(jdbc_type)
		#puts "type = #{@type}"
	end
	
	def jdbc_type_to_type(jdbc_type)
		if jdbc_type == java.sql.Types::VARCHAR || jdbc_type == java.sql.Types::CHAR || jdbc_type == java.sql.Types::LONGNVARCHAR || jdbc_type == java.sql.Types::LONGVARCHAR || jdbc_type == java.sql.Types::NCHAR || jdbc_type == java.sql.Types::NVARCHAR
			:string
		elsif jdbc_type == java.sql.Types::DATE || jdbc_type == java.sql.Types::TIME || jdbc_type == java.sql.Types::TIME_WITH_TIMEZONE || jdbc_type == java.sql.Types::TIMESTAMP || jdbc_type == java.sql.Types::TIMESTAMP_WITH_TIMEZONE
			:date
		elsif jdbc_type == java.sql.Types::BIGINT || jdbc_type == java.sql.Types::BIT || jdbc_type == java.sql.Types::INTEGER || jdbc_type == java.sql.Types::SMALLINT
			:integer
		elsif jdbc_type == java.sql.Types::DECIMAL || jdbc_type == java.sql.Types::FLOAT || jdbc_type == java.sql.Types::DOUBLE || jdbc_type == java.sql.Types::REAL || jdbc_type == java.sql.Types::NUMERIC
			:float
		else
			nil
		end
	end
	
	def to_s
		"#{@pos}: #{@name} #{@type}"
	end
end

class MetaData
	def initialize(meta_data)
		@columns = []
		col_count = meta_data.getColumnCount()
		(1..col_count).each do |pos|
			@columns << Column.new(meta_data, pos)
		end
		if @columns_hash.nil?
			@columns_hash = @columns.inject({}) {|memo, value| memo[value.name] = value; memo}
		end
	end
	
	attr_reader :columns, :columns_hash
end

class JdbcResultSetWrapper
	class << self
		@@retrievers = {
			:date => lambda { |rs, column_name|	rs.getDate(column_name) },
			:integer => lambda { |rs, column_name| rs.getInt(column_name) },
			:float => lambda { |rs, column_name| rs.getDouble(column_name) },
			:string => lambda { |rs, column_name| rs.getString(column_name) }
		}
		
		@@date_format = java.text.SimpleDateFormat.new("yyyy-dd-MM")
	
		@@formatters = {
			:date => lambda do |value|
				formatted_date = @@date_format.format(value)
				"TO_DATE('YYYY-DD-MM', '#{formatted_date}')"
			end,
			:integer => lambda do |value|
				value
			end,
			:float => lambda do |value|
				value
			end,
			:string => lambda do |value|
				escaped_value = value.gsub("'", "''")
				"'#{escaped_value}'"
			end
		}
		
		def retriever(column_type)
			retriever = @@retrievers[column_type]
			retriever = @@retrievers[:string] if retriever.nil?
			retriever
		end
		
		def formatter(column_type)
			formatter = @@formatters[column_type]
			formatter = @@formatters[:string] if formatter.nil?
			formatter
		end
	end

	def initialize(rs, meta_data)
		@rs = rs
		@meta_data = meta_data
	end
	
	def as_string(row_name)
		@rs.getString(row_name)
	end
	
	def [](row_name)
		type = @meta_data.columns_hash[row_name]
		type = :string if type.nil?
		case type
		when :date
			self.as_date(row_name)
		when :integer
			self.as_int(row_name)
		when :float
			self.as_double(row_name)
		else
			self.as_string(row_name)
		end
	end
	
	def as_date(row_name)
		@rs.getDate(row_name)
	end
	
	def as_time(row_name)
		@rs.getTime(row_name)
	end
	
	def as_int(row_name)
		@rs.getInt(row_name)
	end
	
	def as_double(row_name)
		@rs.getDouble(row_name)
	end
	
	def retrieve_values(columns)
		values = []
		columns.each do |column|
			name = column[0]
			col_type = column[1]
			retriever = JdbcResultSetWrapper.retriever(col_type)
			value = retriever.call(@rs, name)
			values << value
		end
		values
	end
	
	def row_as_insert(table_name, columns)
		values = retrieve_values(columns)
		
		count = 0
		insert_columns = []
		insert_values = []
		
		columns.each do |column|
			name = column[0]
			col_type = column[1]
			value = values[count]
			if !value.nil?
				insert_columns << name
				formatter = JdbcResultSetWrapper.formatter(col_type)
				insert_values << formatter.call(value)
			end
			count += 1
		end
		
		s = "insert into #{table_name} (#{insert_columns.join(",")})\nvalues(#{insert_values.join(",")})"
		s
	end
	
	def to_hash()
		@meta_data.columns.inject({}) {|memo, value| memo[value.name] = self[value.name]; memo}
	end
	
	def columns()
		@meta_data.columns
	end
end

class JdbcConnection

	class << self
		def db_connect(url, user, pass, driver, &block)
			con = nil
			begin
				#java.lang.Class.forName(driver, true, JRuby.runtime.jruby_class_loader) # see http://www.ruby-forum.com/topic/209741#912610
				java_import driver
				con = java.sql.DriverManager.getConnection(url, user, pass)
				yield JdbcConnection.new(con)
			ensure
				begin
					con.close unless con.nil?
				ensure
					con = nil
				end
			end
		end
	end
	
	def dump_table(table_name, where_clause, columns, &block)
		column_names = columns.collect {|column| column[0]}.join(",")
		q = "select #{column_names} from #{table_name} where #{where_clause}"
		query(q) do |row|
			statement = row.row_as_insert(table_name, columns)
			yield statement
		end
	end
	
	def query(query, *args, &block)
		stmt = @con.prepareStatement(query)
		add_args_to_stmt!(stmt, args) if args.length > 0
		rs = stmt.executeQuery
		meta_data = MetaData.new(rs.getMetaData())
		while rs.next
			yield JdbcResultSetWrapper.new(rs, meta_data)
		end
		rs.close
		stmt.close
	end
	
	def table_to_map(table_name, key_field, value_field)
		m = {}
		q = "select #{key_field}, #{value_field} from #{table_name}"
		query(q) do |row|
			m[row.getString(key_field)] = row.getString(value_field)
		end
		m
	end
	
	private
	
	def initialize(con)
		@con = con
	end
	
	def add_args_to_stmt!(stmt, args)
		count = 1
		args.each do |arg|
			if arg.nil?
				stmt.setNull(count)
			elsif arg.class == String
				stmt.setString(count, arg)
			elsif arg.class == Fixnum || arg.class == Integer
				stmt.setLong(count, arg)
			end
			count += 1
		end
	end
	
end

class MysqlJdbcConnection < JdbcConnection
	def current_datetime_function()
		"now()"
	end
	
	def generate_update_sequence_sql(sequence_name)
		
	end
	
	def generate_sequence_nextval_placeholder(sequence_name)
		
	end
end

class OracleJdbcConnection < JdbcConnection
	def current_datetime_function()
		"SYSTEM"
	end
	
	def generate_update_sequence_sql()
		
	end
	
	def generate_sequence_nextval_placeholder()
		
	end
end

def find_workgroup_id(workgroup_name, con)
	workgroup_id = nil
	con.complex_query("select wrkgrp_id from en_wrkgrp_t where wrkgrp_nm = ? and wrkgrp_actv_ind = 1 and wrkgrp_cur_ind = 1", workgroup_name) do |rs|
		workgroup_id = rs.getLong("wrkgrp_id")
	end
	if workgroup_id.nil?
		$stderr.write("Cannot find workgroup record for #{workgroup_name}\n")
	end
	workgroup_id
end

def escape_string(str)
	str = str.gsub("'", "''") unless str.nil?
	if str.nil? || str.length == 0
		"null"
	else
		"'#{str}'"
	end
end

def db_connect(connection_values, &block)
	def read_connection_values(connection_values)
		values_yaml = nil
		File.open("#{connection_values}.yml", "r") do |fin|
			values_yaml = YAML::load(fin)
		end
		values_yaml
	end
	
	def determine_driver(db_url)
		if db_url =~ /^jdbc:mysql:/ 
			"com.mysql.jdbc.Driver"
		elsif db_url =~ /^jdbc:oracle:thin/ 
			"oracle.jdbc.OracleDriver"
		else
			nil
		end
	end
	
	db_connection_values = read_connection_values(connection_values)
	JdbcConnection.db_connect(db_connection_values["url"], db_connection_values["user"], db_connection_values["pass"], determine_driver(db_connection_values["url"])) do |con|
		yield con
	end
end