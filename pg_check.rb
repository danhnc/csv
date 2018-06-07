#!/usr/bin/ruby

require 'pg'

# if ARGV.length != 1 then
#     puts "Usage: prepared_statement.rb rowId"
#     exit
# end

# rowId = ARGV[0]

begin
	path = "/home/local/ELARION/danhnc/training/ruby/sample.csv"
	con = PG.connect :dbname => 'danhnc', :user => 'danhnc'
    id = 1

    filename = File.basename(path, ".*")
	row = File.readlines(path)
    con.exec "DROP TABLE IF EXISTS #{filename}"
    att_name = row[0].split(',')
    puts att_name.size
    # puts att_name[0]
    # con.exec "CREATE TABLE #{filename} (#{att_name[0]}  INTEGER PRIMARY KEY)"
    con.exec "CREATE TABLE #{filename} (id INTEGER PRIMARY KEY)"
    att_index = 0
    puts "================="
    while att_index < att_name.size do
        puts att_name[att_index]
    	con.exec "ALTER TABLE #{filename} ADD #{att_name[att_index]} TEXT"
    	att_index += 1
    end
    row_index = 1
    while row_index < row.size do
    	col = row[row_index].split(',')
    	con.exec "INSERT INTO #{filename} VALUES(#{id})"
    	# break
    	col_index = 0
    	while col_index < col.size do
    		# con.exec "UPDATE #{filename} SET #{att_name[col_index]}='#{col[col_index]}' WHERE #{att_name[0]}=#{row_index}"
            con.exec "UPDATE #{filename} SET #{att_name[col_index]}='#{col[col_index]}' WHERE #{id}=#{row_index+1}"
    		col_index += 1
    	end
    	row_index += 1
        id += 1
    end
    
rescue PG::Error => e
	puts e.message
ensure

	# rs.clear if rs
	con.close if con
end