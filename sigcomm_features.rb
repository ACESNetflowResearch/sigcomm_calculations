#!/usr/bin/ruby
#Here, we can directly obtain the features: duration, protocol, bytes per packet, number of packets, number of bytes    
#Some sigcomm features ask for max, min, std dev...I am calculating this per IP
require 'mysql'

begin
    con = Mysql.new 'localhost', 'root', 'cyberaces', 'netflow_db' #open connection
    all_src_ips = con.query("SELECT src_IP FROM netflow_full") #get all the distinct src IPS
    #for each src IP, caclulate it's statistics by grabbing all its flows and processing them
    all_src_ips.each{ |ip|
	# features that can be directly pulled from netflow:   
	   duration=0
	   protocol=""
	   num_packets=0
	   num_bytes=0
        bpp=0

	   ip_data = con.query("SELECT * FROM netflow_full WHERE src_IP = '#{ip[0]}'")  
	   ip_data.each{|flow|
		  duration = flow[1]
		  protocol=flow[2]  
		  num_packets = flow[9] 
		  num_bytes=flow[10]
		  bpp = flow[13]
	   }
	   puts "#{ip[0]} duration: #{duration}, protocol: #{protocol}, number of packets: #{num_packets}, number of bytes: #{num_bytes}, number of bytes per packet: #{bpp}"
    }

rescue Mysql::Error => e
    puts e.errno
    puts e.error
ensure
    con.close if con
end   
