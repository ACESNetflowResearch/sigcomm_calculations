#!/usr/bin/ruby
#Here, we can directly obtain the features: duration, protocol, bytes per packet, number of packets, number of bytes    
#Some sigcomm features ask for max, min, std dev...I am calculating this per IP
require 'mysql'

class Calculate_sigcomm_features
    attr_accessor :ctr, :min_packets, :max_packets, :packets_total, :std_packets_sum, :std_packets
    @ctr = 0
    @min_packets= nil
    @max_packets= nil
    @packets_total=0.0
    @std_packets_sum=0.0
    @std_packets=0.0

    def packet_stats(packets)
	   if min_packets == nil
		  min_packets = packets
	   elsif min_packets > packets
		  min_packets = packets
	   end
	   
	    if max_packets == nil
		  max_packets = packets
	   elsif max_packets < packets
		  max_packets = packets
	   end
	    
#	   packets_total = packets_total + packets
#	   std_packets_sum = std_packets_sum + (packets - (packets_total/ctr))*(packets - (packets_total/ctr))
    end 

    #def duration_stats
    #end

    #def bpp_stats
    #end

    #def bytes_stats
    #end

end



begin
    con = Mysql.new 'localhost', 'root', 'cyberaces', 'netflow_db' #open connection
    all_src_ips = con.query("SELECT DISTINCT src_IP FROM netflow_full") #get all the distinct src IPS
    #for each src IP, caclulate it's statistics by grabbing all its flows and processing them
    all_src_ips.each{ |ip|
     # features that require calculations
	   
	   #min_packets= nil
	   #max_packets= nil
	   #packets_total=0.0
	   #std_packets_sum=0.0
	   #std_packets=0.0

	   min_duration= nil
	   max_duration= nil
	   duration_total=0.0
	   std_duration_sum=0.0
	   std_duration=0.0
	   
	   min_bpp = nil
	   max_bpp = nil
	   bpp_total=0.0
	   std_bpp_sum=0.0
	   std_bpp= 0.0

	   min_bytes = nil
	   max_bytes = nil
	   bytes_total=0.0
	   std_bytes_sum=0.0
	   std_bytes=0.0

	   c.ctr = 0
	   ip_data = con.query("SELECT * FROM netflow_full WHERE src_IP = '#{ip[0]}'")  
	   ip_data.each{|flow|
		  c = Calculate_sigcomm_features.new
		  c.ctr = c.ctr + 1
		  c.packets_total = 0.0
		  c.std_packets_sum = 0.0
		  c.std_packets = 0.0
		  #puts c.packets_total
		  #min_packets = nil
		  #max_packets = nil
		  #packets_total = 0.0
		  #std_packets_sum = 0.0
		  #std_packets_sum = 0.0

		  c.packet_stats(flow[9].to_f)
		  #duration = flow[1]  
		  #num_packets = flow[9] 
		  #num_bytes=flow[10]
		  #bpp = flow[13]
	   

	   #calculations for packet number per IP
		  #if min_packets == nil
		#	 min_packets = flow[9].to_f
		 # elsif min_packets > flow[9].to_f
		#	 min_packets = flow[9].to_f
		  #end

		  
		  #if max_packets == nil
			 #	 max_packets = flow[9].to_f
		  #elsif max_packets < flow[9].to_f
            #    max_packets = flow[9].to_f
		  #end
    
		  #packets_total = packets_total + flow[9].to_f
		  #std_packets_sum = std_packets_sum + (flow[9].to_f - (packets_total/ctr))*(flow[9].to_f - (packets_total/ctr))

	   #calculations for duration per IP
		  if min_duration == nil
			 min_duration = flow[1].to_f
		  elsif min_duration > flow[1].to_f
			 min_duration = flow[1].to_f
		  end
		            
		  
		  if max_duration == nil
			 max_duration = flow[1].to_f
		  elsif max_duration < flow[1].to_f
			 max_duration = flow[1].to_f
		  end
		  duration_total = duration_total + flow[1].to_f
		  std_duration_sum = std_duration_sum + (flow[1].to_f - (duration_total/ctr))*(flow[1].to_f - (duration_total/ctr))
	   
	   #calcultions for bytes per packet per IP
		  if min_bpp == nil
			 min_bpp = flow[13].to_f
		  elsif min_bpp > flow[13].to_f
			 min_bpp = flow[13].to_f
		  end

		  
		  if max_bpp == nil
			 max_bpp = flow[13].to_f
		  elsif max_bpp < flow[13].to_f
			 max_bpp = flow[13].to_f
		  end
		  bpp_total = bpp_total + flow[13].to_f
		  std_bpp_sum = std_bpp_sum + (flow[13].to_f - (bpp_total/ctr))*(flow[13].to_f - (bpp_total/ctr))

	   #calculation for bytes per IP
		  if min_bytes == nil
			 min_bytes = flow[10].to_f
		  elsif min_bytes > flow[10].to_f
			 min_bytes = flow[10].to_f
		  end 

		  
		  if max_bytes == nil
			 max_bytes = flow[10].to_f
	       elsif max_bytes < flow[10].to_f
			 max_bytes = flow[10].to_f
		  end 
		  bytes_total = bytes_total + flow[10].to_f
		  std_bytes_sum = std_bytes_sum + (flow[10].to_f - (bytes_total/ctr))*(flow[10].to_f - (bytes_total/ctr))
	   }
	   
	   std_duration = Math.sqrt(std_duration_sum/ctr)
	   std_bytes = Math.sqrt(std_bytes_sum/ctr)
	   std_packets = Math.sqrt(std_packets_sum/ctr)
	   std_bpp = Math.sqrt(std_bpp_sum/ctr)

	   puts "#{ip[0]} min duration: #{min_duration}, max duration: #{max_duration}, mean duration: #{duration_total/ctr}, standard deviation of duration: #{std_duration}" 
	   puts "#{ip[0]} min bytes: #{min_bytes}, max bytes: #{max_bytes}, mean bytes: #{bytes_total/ctr}, standard deviation of bytes: #{std_bytes}" 
	   puts "#{ip[0]} min packets: #{min_packets}, max packets: #{max_packets}, mean packets: #{packets_total/ctr}, standard deviation of packets: #{std_packets}" 
	   puts "#{ip[0]} min bpp: #{min_bpp}, max bpp: #{max_bpp}, mean bpp: #{bpp_total/ctr}, standard deviation of bpp: #{std_bpp}"
    }

rescue Mysql::Error => e
    puts e.errno
    puts e.error
ensure
    con.close if con
end   
