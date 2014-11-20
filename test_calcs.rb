#!/usr/bin/ruby
require 'mysql'

begin
con = Mysql.new 'localhost', 'root', 'cyberaces', 'netflow_db' #open connection
all_src_ips = con.query("SELECT DISTINCT src_IP FROM netflow_full")
all_src_ips.each{ |ip|
    ctr = 0 
    min_packets= nil
    max_packets= nil
    packets_total=0.0
    std_packets_sum=0.0
    std_packets=0.0

    ip_data = con.query("SELECT * FROM netflow_full WHERE src_IP = '#{ip[0]}'")
      ip_data.each{|flow|
	     #calculations for packet number per IP
		ctr = ctr + 1
		if min_packets == nil
		  min_packets = flow[9].to_f
		elsif min_packets > flow[9].to_f
		  min_packets = flow[9].to_f
		end

	     if max_packets == nil
		  max_packets = flow[9].to_f
		elsif max_packets < flow[9].to_f
		  max_packets = flow[9].to_f
		end 
		packets_total = packets_total + flow[9].to_f
		std_packets_sum = std_packets_sum + (flow[9].to_f - (packets_total/ctr))*(flow[9].to_f - (packets_total/ctr)) 

	 }
    std_packets = Math.sqrt(std_packets_sum/ctr)
    puts "#{ip[0]} min packets: #{min_packets}, max packets: #{max_packets}, mean packets: #{packets_total/ctr}, standard devi    ation of packets: #{std_packets}"
    }
rescue Mysql::Error => e
    puts e.errno
    puts e.error
ensure
    con.close if con
end
