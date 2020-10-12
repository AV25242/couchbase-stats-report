require 'couchbase'
require 'uri'
require 'json'
require 'date'
require 'open3'
require 'gruff'

require_relative 'storage'


def getstats(rstart,rend,repositories = nil)

  syear = rstart[0,4].to_i
  eyear = rend[0,4].to_i
  smonth  = rstart[4,2].to_i
  emonth  = rend[4,2].to_i

  options = Couchbase::Cluster::QueryOptions.new

  options.metrics = true
  options.named_parameters({p1: syear,p2: smonth, p3: eyear, p4: emonth})

  repo = ""

  if(repositories != nil) then

    repositories.each{ |item|
      repo = repo + "'" + item +"',"
    }

    repo = repo.chomp(',')
  end

  type = (repositories == nil) ? "" : " AND s.stats.type IN [#{repo}]"

  where  = (syear == eyear) ? "(s.stats.year = $p1 AND (s.stats.month >= $p2 AND s.stats.month <= $p4))"
  : "(s.stats.year = $p1 AND s.stats.month >= $p2) OR (s.stats.year = $p3 and s.stats.month <= $p4)"

  where  = where + type

  res = Storage.instance.cluster.query("SELECT s.stats FROM #{Storage.instance.bucket} s WHERE (#{where})  order by s.stats.year,s.stats.month ", options)

  clients = []

  res.rows.each {
  |row|
   clients << row["stats"]
  }

  return clients
end

def generate_report(mstart, mend, report_type="")
  case report_type
  when "1"
    stats =  getstats(mstart,mend, ["maven","npm","maven-spring"])
    monthly_connector_report(stats,mend)
  when "2"
    stats =  getstats(mstart,mend, ["npm"])
    marvel_trend_report(stats,mend)
  when "3"
    stats =  getstats(mstart,mend)
    quarterly_sdk_report(stats,mend)
  when "4"
    stats =  getstats(mstart,mend, ["maven","npm","maven-spring"])
    quarterly_connector_report(stats,mend)
  when "5"
    stats =  getstats(mstart,mend, ["maven"])
    transactions_trend_report(stats,mend)
  when "10"
    stats =  getstats(mstart,mend)
    monthly_sdk_report(stats,mend)
  else
      stats =  getstats(mstart,mend)
      monthly_connector_report(stats,mend)
      marvel_trend_report(stats,mend)
      monthly_sdk_report(stats,mend)
      quarterly_sdk_report(stats,mend)
      quarterly_connector_report(stats,mend)
      transactions_trend_report(stats,mend)
  end
end

def monthly_sdk_report(stats,mend)

  filepath = "reports/monthly_#{mend}_sdk.png"

  g = Gruff::Bar.new('800x500') # Define a custom size
  g.sort = false  # Do NOT sort data based on values
  #g.theme = Gruff::Themes::RAILS_KEYNOTE
  g.title = 'SDK Download Stats(Ext. Packages)'

  monthname = ["Jan", "Feb", "Mar", "Apr", "May", "Jun","Jul","Aug","Sep", "Oct", "Nov", "Dec"]


  stats = stats.sort_by {|a| [a['year'], a['month']]}
  pmonth = 0
  pyear = ""

  monthlystats = [0,0,0,0,0,0,0]
  maxval = 0
  minval = 1000000000

  nugetpiormonthstats = 0
  rubypriormonthstats = 0

  stats.each_with_index {
    |item, index|

     client =  item["client"]
     type = item["type"]

     cmonth = item["month"]
     cyear  = item["year"]
     pyear = (pyear =="") ? cyear : pyear

     if(cmonth != pmonth ) then

       if(pmonth != 0) then
           label  = monthname[pmonth-1] + pyear.to_s[2,4]

           g.data(label, monthlystats)

           maxval = (monthlystats.max > maxval) ? monthlystats.max : maxval
           minval = (monthlystats.min < minval) ? monthlystats.min : minval

           nugetpiormonthstats = monthlystats[0]
           rubypriormonthstats = monthlystats[4]
           monthlystats = [0,0,0,0,0,0,0]
       end
       pmonth = cmonth
       pyear = cyear
     end

     if (type == "nuget") then
       net = client.find {|h| h.member? "CouchbaseNetClient3x"}
       if net then
          monthlystats[0] = (net["CouchbaseNetClient3x"].to_i - nugetpiormonthstats).abs()
       end
     end

     if (type == "maven") then
       java = client.find {|h| h.member? "couchbase-client"}
       if java then
          monthlystats[1] = java["couchbase-client"].to_i
       end

       scala = client.find {|h| h.member? "scala"}
       if scala then
          monthlystats[3] = scala["scala"].to_i
       end
     end

     if (type == "npm") then

       node = client.find {|h| h.member? "node-sdk"}
       if node then
          monthlystats[2] = node["node-sdk"].to_i
       end
     end

     if (type == "rubygems") then
       ruby = client.find {|h| h.member? "ruby"}
       if ruby then
          monthlystats[4] = (ruby["ruby"].to_i - rubypriormonthstats).abs()
       end
     end
=begin
     if (type == "pypi") then
       python = client.find {|h| h.member? "python-sdk3.x"}
       if python then
          #monthlystats[5] = (python["python-sdk3.x"].to_i - pypriormonthstats).abs()
          monthlystats[5] = (item["total"].to_i - pypriormonthstats).abs()
       end
     end
=end
    if (type == "pypi-bigquery") then
      python = client.find {|h| h.member? "python-sdk"}
      if python then
         #monthlystats[5] = (python["python-sdk3.x"].to_i - pypriormonthstats).abs()
         monthlystats[5] = python["python-sdk"].to_i

      end
    end

    if (type == "gocenter") then
       golang = client.find {|h| h.member? "go"}
       if golang then
          monthlystats[6] = golang["go"].to_i
       end
    end

    if(stats.length-1 == index) then
        label  = monthname[pmonth-1] + cyear.to_s[2,4]
        g.data(label, monthlystats)
    end

  }

  g.maximum_value = maxval  # Declare a max value for the Y axis
  g.minimum_value = minval   # Declare a min value for the Y axis

  g.theme = {   # Declare a custom theme
    :colors => %w(orange purple green white red blue cyan yellow brown indigo), # colors can be described on hex values (#0f0f0f)
    :marker_color => 'black', # The horizontal lines color
    :background_colors => %w(white grey) # you can use instead: :background_image => ‘some_image.png’
  }
  g.y_axis_increment = 5000
  g.legend_font_size = 12 # Legend font size
  g.title_font_size = 12 # Title font size
  g.marker_font_size = 14
  g.legend_box_size = 10.0

  g.labels = {0 => '.Net', 1 => 'Java', 2 => 'Node.js', 3 => 'Scala', 4 => 'Ruby',5 => 'Python', 6 => 'Go'}

  g.write(filepath)

end

def monthly_connector_report(stats,mend)

  filepath = "reports/monthly_#{mend}_connector.png"

  g = Gruff::Bar.new('800x500') # Define a custom size
  g.sort = false  # Do NOT sort data based on values
  #g.theme = Gruff::Themes::RAILS_KEYNOTE
  g.title = 'Connector/Framework Download Stats'

  monthname = ["Jan", "Feb", "Mar", "Apr", "May", "Jun","Jul","Aug","Sep", "Oct", "Nov", "Dec"]


  stats = stats.sort_by {|a| [a['year'], a['month']]}
  pmonth = 0

  monthlystats = [0,0,0,0,0]
  maxval = 0
  minval = 1000000000

  stats.each_with_index {
    |item, index|

     client =  item["client"]
     type = item["type"]

     cmonth = item["month"]

     if(cmonth != pmonth) then

       if(pmonth != 0) then
           label  = monthname[pmonth-1] + item["year"].to_s[2,4]

           g.data(label, monthlystats)

           maxval = (monthlystats.max > maxval) ? monthlystats.max : maxval
           minval = (monthlystats.min < minval) ? monthlystats.min : minval

           monthlystats = [0,0,0,0,0]
       end
       pmonth = cmonth
     end

     if (type == "npm") then

       ottoman = client.find {|h| h.member? "ottoman"}
       if ottoman then
          monthlystats[3] = ottoman["ottoman"].to_i
       end
     end
     if (type == "maven-spring") then

       springdata = client.find {|h| h.member? "spring-data"}
       if springdata then
          monthlystats[4] = springdata["spring-data"].to_i
       end
     end
     if (type == "maven") then
       kafka = client.find {|h| h.member? "kafka-connector"}
       if kafka then
          monthlystats[0] = kafka["kafka-connector"].to_i
       end

       spark = client.find {|h| h.member? "spark-connector"}
       if spark then
          monthlystats[1] = spark["spark-connector"].to_i
       end

       dcp = client.find {|h| h.member? "dcp-client"}
       if dcp then
          monthlystats[2] = dcp["dcp-client"].to_i
       end
     end
     if(stats.length-1 == index) then
         label  = monthname[pmonth-1] + item["year"].to_s[2,4]
         g.data(label, monthlystats)
     end
  }

  g.maximum_value = maxval  # Declare a max value for the Y axis
  g.minimum_value = minval   # Declare a min value for the Y axis

  g.theme = {   # Declare a custom theme
    :colors => %w(orange purple green white red blue cyan yellow brown indigo), # colors can be described on hex values (#0f0f0f)
    :marker_color => 'black', # The horizontal lines color
    :background_colors => %w(white grey) # you can use instead: :background_image => ‘some_image.png’
  }
  g.y_axis_increment = 2500
  g.legend_font_size = 12 # Legend font size
  g.title_font_size = 12 # Title font size
  g.marker_font_size = 14
  g.legend_box_size = 10.0

  g.labels = {0 => 'Kafka', 1 => 'Spark', 2 => 'DCP', 3 => 'Ottoman', 4 => 'Spring-Data'}

  g.write(filepath)

end

def marvel_trend_report(stats,mend)

  filepath = "reports/marvel_#{mend}_trend.png"

  monthname = ["Jan", "Feb", "Mar", "Apr", "May", "Jun","Jul","Aug","Sep", "Oct", "Nov", "Dec"]

  stats = stats.sort_by {|a| [a['year'], a['month']]}

  pmonth = 0
  pyear = ""
  odownloads = []
  label = Hash.new(0)

  stats.each_with_index {
    |item, index|

     client =  item["client"]
     type = item["type"]
     cmonth = item["month"]
     cyear  = item["year"].to_s[2,4]
     pyear = (pyear =="") ? cyear : pyear

     if(cmonth != pmonth || stats.length-1 == index) then
      label[label.length] = monthname[cmonth-1] + pyear
       pmonth = cmonth
       pyear = cyear
     end

     if (type == "npm") then

       ottoman = client.find {|h| h.member? "ottoman"}
       if ottoman then
          odownloads << ottoman["ottoman"].to_i
       end
     end

  }

  minval = 0
  maxval  = odownloads.max

  g = Gruff::Line.new('800x500')
    #g = Gruff::Bar.new('800x500') # Define a custom size
  g.title = 'Marvel Download Stats'
  g.labels = label
  g.data :Ottoman, odownloads

  #g.maximum_value = maxval  # Declare a max value for the Y axis
  g.minimum_value = minval   # Declare a min value for the Y axis

  g.theme = {   # Declare a custom theme
    :colors => %w(orange purple green white red blue cyan yellow brown indigo), # colors can be described on hex values (#0f0f0f)
    :marker_color => 'black', # The horizontal lines color
    :background_colors => %w(white grey) # you can use instead: :background_image => ‘some_image.png’
  }
  g.y_axis_increment = 100
  g.legend_font_size = 14 # Legend font size
  g.title_font_size = 14 # Title font size
  g.marker_font_size = 14
  g.legend_box_size = 15.0
  g.y_axis_label = 'Count'

  g.write(filepath)

end

def quarterly_sdk_report(stats,mend)

  filepath = "reports/quaterly_#{mend}_sdk.png"

  g = Gruff::Bar.new('800x500') # Define a custom size
  g.sort = false  # Do NOT sort data based on values
  #g.theme = Gruff::Themes::RAILS_KEYNOTE
  g.title = 'SDK Quarterly Download Trends'

  monthname = ["Jan", "Feb", "Mar", "Apr", "May", "Jun","Jul","Aug","Sep", "Oct", "Nov", "Dec"]
  quarter  = { 3 => "Q1", 6 => "Q2", 9 => "Q3", 12 => "Q4" }
  quartermonths  = { 1 => "Q1",2 => "Q1", 3 => "Q1", 4 => "Q2", 5 => "Q2",6 => "Q2",7 => "Q3", 8 => "Q3", 9 => "Q3",10 => "Q4",11 => "Q4", 12 => "Q4" }

  stats = stats.sort_by {|a| [a['year'], a['month']]}

  pmonth = 0
  pyear  = ""

  monthlystats = [0,0,0,0,0,0,0]
  maxval = 0
  minval = 1000000000


  #prior month count
  netpmcount,rbpmcount, pypmcount  = 0,0,0

  netcount,jacount,njcount,slcount,rbcount,pycount,gocount = 0,0,0,0,0,0,0

  stats.each_with_index {
    |item, index|

     client =  item["client"]
     type = item["type"]

     cmonth = item["month"]
     cyear  = item["year"]
     pyear = (pyear =="") ? cyear : pyear

     if (type == "nuget") then
         net = client.find {|h| h.member? "CouchbaseNetClient3x"}
         if net then
            netcount = netcount + ( net["CouchbaseNetClient3x"].to_i - netpmcount).abs()
            #netcount = netcount + net["CouchbaseNetClient3x"].to_i
            netpmcount = net["CouchbaseNetClient3x"].to_i
         end
     end

     if (type == "maven") then
       java = client.find {|h| h.member? "couchbase-client"}
       if java then
          jacount = jacount + java["couchbase-client"].to_i
       end

       scala = client.find {|h| h.member? "scala"}
       if scala then
          slcount = slcount +  scala["scala"].to_i
       end
     end

     if (type == "npm") then

       node = client.find {|h| h.member? "node-sdk"}
       if node then
          njcount = njcount + node["node-sdk"].to_i
       end
     end

     if (type == "rubygems") then
       ruby = client.find {|h| h.member? "ruby"}
       if ruby then
        rbcount = rbcount + ( ruby["ruby"].to_i - rbpmcount).abs()
        rbpmcount = ruby["ruby"].to_i
       end
     end

=begin
     if (type == "pypi") then
       python = client.find {|h| h.member? "python-sdk3.x"}
       if python then
          pycount = pycount + (item["total"].to_i - pypmcount).abs()
          pypmcount = item["total"].to_i
       end
     end
=end
    if (type == "pypi-bigquery") then
      python = client.find {|h| h.member? "python-sdk"}
      if python then
           pycount = pycount + python["python-sdk"].to_i

      end
    end

    if (type == "gocenter") then
       golang = client.find {|h| h.member? "go"}
       if golang then
          gocount = gocount +  golang["go"].to_i
       end
    end

     if(cmonth != pmonth || stats.length-1 == index ) then

     if (pmonth !=0 ) then


        q = quarter.find {|h| h.member? pmonth}
         if q || stats.length-1 == index then
           if q == nil then
                 qtag  = quartermonths[pmonth]
           else
                 qtag  = q[1]
           end

           label  = + pyear.to_s + "-#{qtag}"
          #  puts "month of #{pmonth} with index of #{index} and maxlength is #{stats.length} and python count is #{pycount}"

           monthlystats = [netcount,jacount,njcount,slcount,rbcount,pycount,gocount]
           g.data(label, monthlystats)

           maxval = (monthlystats.max > maxval) ? monthlystats.max : maxval
           minval = (monthlystats.min < minval) ? monthlystats.min : minval

           monthlystats = [0,0,0,0,0,0,0]

           netcount,jacount,njcount,slcount,rbcount,pycount,gocount = 0,0,0,0,0,0,0

         end
     end
     pmonth = cmonth
    pyear = cyear
   end
  }

  g.maximum_value = maxval  # Declare a max value for the Y axis
  g.minimum_value = minval   # Declare a min value for the Y axis


  g.theme = {   # Declare a custom theme
    :colors => %w(darkblue olive skyblue green brown orange blue cyan  Turquoise indigo), # colors can be described on hex values (#0f0f0f)
    :marker_color => 'black', # The horizontal lines color
    :background_colors => %w(white grey) # you can use instead: :background_image => ‘some_image.png’
  }
  g.y_axis_increment = 25000
  g.legend_font_size = 12 # Legend font size
  g.title_font_size = 12 # Title font size
  g.marker_font_size = 14
  g.legend_box_size = 10.0

  g.labels = {0 => '.Net', 1 => 'Java', 2 => 'Node.js', 3 => 'Scala', 4 => 'Ruby',5 => 'Python', 6 => 'Go'}

  g.write(filepath)

end

def quarterly_connector_report(stats,mend)

  filepath = "reports/quaterly_#{mend}_connector.png"

  g = Gruff::Bar.new('800x500') # Define a custom size
  g.sort = false  # Do NOT sort data based on values
  #g.theme = Gruff::Themes::RAILS_KEYNOTE
  g.title = 'Connector/Framework Quarterly Download Trends'

  monthname = ["Jan", "Feb", "Mar", "Apr", "May", "Jun","Jul","Aug","Sep", "Oct", "Nov", "Dec"]
  quarter  = { 3 => "Q1", 6 => "Q2", 9 => "Q3", 12 => "Q4" }
  quartermonths  = { 1 => "Q1",2 => "Q1", 3 => "Q1", 4 => "Q2", 5 => "Q2",6 => "Q2",7 => "Q3", 8 => "Q3", 9 => "Q3",10 => "Q4",11 => "Q4", 12 => "Q4" }

  stats = stats.sort_by {|a| [a['year'], a['month']]}

  pmonth = 0
  pyear  = ""

  monthlystats = [0,0,0,0,0]
  maxval = 0
  minval = 1000000000

  kafkacount,sparkcount,dcpcount,ottomancount,springdatacount = 0,0,0,0,0

  stats.each_with_index {
    |item, index|

     client =  item["client"]
     type = item["type"]

     cmonth = item["month"]
     cyear  = item["year"]
     pyear = (pyear =="") ? cyear : pyear

     if (type == "maven") then
       kafka = client.find {|h| h.member? "kafka-connector"}
       if kafka then
          kafkacount = kafkacount + kafka["kafka-connector"].to_i
       end

       spark = client.find {|h| h.member? "spark-connector"}
       if spark then
          sparkcount = sparkcount + spark["spark-connector"].to_i
       end

       dcp = client.find {|h| h.member? "dcp-client"}
       if dcp then
          dcpcount = dcpcount + dcp["dcp-client"].to_i
       end
     end

     if (type == "npm") then

       node = client.find {|h| h.member? "ottoman"}
       if node then
          ottomancount = ottomancount + node["ottoman"].to_i
       end
     end
     if (type == "maven-spring") then
       springdata = client.find {|h| h.member? "spring-data"}
       if springdata then
          springdatacount = springdatacount + springdata["spring-data"].to_i
       end
     end

     if(cmonth != pmonth || stats.length-1 == index ) then

       if (pmonth !=0 ) then
          q = quarter.find {|h| h.member? pmonth}
           if q || stats.length-1 == index then
             if q == nil then
                   qtag  = quartermonths[pmonth]
             else
                   qtag  = q[1]
             end

             label  = + pyear.to_s + "-#{qtag}"

             monthlystats = [kafkacount,sparkcount,dcpcount,ottomancount,springdatacount]
             g.data(label, monthlystats)

             maxval = (monthlystats.max > maxval) ? monthlystats.max : maxval
             minval = (monthlystats.min < minval) ? monthlystats.min : minval

             monthlystats = [0,0,0,0]

            kafkacount,sparkcount,dcpcount,ottomancount,springdatacount = 0,0,0,0,0
           end
       end
       pmonth = cmonth
       pyear = cyear
   end
  }

  g.maximum_value = maxval  # Declare a max value for the Y axis
  g.minimum_value = minval   # Declare a min value for the Y axis


  g.theme = {   # Declare a custom theme
    :colors => %w(darkblue olive skyblue green brown orange blue cyan  Turquoise indigo), # colors can be described on hex values (#0f0f0f)
    :marker_color => 'black', # The horizontal lines color
    :background_colors => %w(white grey) # you can use instead: :background_image => ‘some_image.png’
  }
  g.y_axis_increment = 5000
  g.legend_font_size = 12 # Legend font size
  g.title_font_size = 12 # Title font size
  g.marker_font_size = 14
  g.legend_box_size = 10.0

  g.labels = {0 => 'Kafka', 1 => 'Spark', 2 => 'DCP', 3 => 'Ottoman', 4 => 'Spring-Data'}

  g.write(filepath)

end

def transactions_trend_report(stats,mend)

  filepath = "reports/transactions_#{mend}_trend.png"

  monthname = ["Jan", "Feb", "Mar", "Apr", "May", "Jun","Jul","Aug","Sep", "Oct", "Nov", "Dec"]

  stats = stats.sort_by {|a| [a['year'], a['month']]}

  pmonth = 0
  pyear = ""
  odownloads = []
  label = Hash.new(0)

  stats.each_with_index {
    |item, index|

     client =  item["client"]
     type = item["type"]
     cmonth = item["month"]
     cyear  = item["year"].to_s[2,4]
     pyear = (pyear =="") ? cyear : pyear

     if(cmonth != pmonth || stats.length-1 == index) then
      label[label.length] = monthname[cmonth-1] + pyear
       pmonth = cmonth
       pyear = cyear
     end

     if (type == "maven") then

       jtransaction = client.find {|h| h.member? "couchbase-transactions"}
       if jtransaction then
          odownloads << jtransaction["couchbase-transactions"].to_i
       end
     end


  }

  minval = 0
  maxval  = odownloads.max

  g = Gruff::Line.new('800x500')
    #g = Gruff::Bar.new('800x500') # Define a custom size
  g.title = 'Transactions Download Stats'
  g.labels = label
  g.data :Java, odownloads

  #g.maximum_value = maxval  # Declare a max value for the Y axis
  g.minimum_value = minval   # Declare a min value for the Y axis

  g.theme = {   # Declare a custom theme
    :colors => %w(brown purple green orange white red blue cyan yellow brown indigo), # colors can be described on hex values (#0f0f0f)
    :marker_color => 'black', # The horizontal lines color
    :background_colors => %w(white grey) # you can use instead: :background_image => ‘some_image.png’
  }
  g.y_axis_increment = 100
  g.legend_font_size = 14 # Legend font size
  g.title_font_size = 14 # Title font size
  g.marker_font_size = 14
  g.legend_box_size = 15.0
  g.y_axis_label = 'Count'

  g.write(filepath)

end



generate_report('202001','202010',"4")
