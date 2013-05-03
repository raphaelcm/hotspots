require 'bundler/setup'
require 'sinatra'
require 'moped'
require 'json'

Page = Struct.new(:i, :path, :visitors) do
  def to_json(*a)
    {
        i: i,
        path: path,
        visitors: visitors
    }.to_json(*a)
  end
end

def extract_results(raw)
  results = {}

  raw.each do |toppage|
    toppage.each do |page|
      if results[page["path"]].nil?
        results[page["path"]] = Page.new(page["i"], page["path"], page["visitors"])
      else
        results[page["path"]].visitors += page["visitors"]
      end
    end
  end

  results.values.each{|v| v.visitors = v.visitors/raw.length} # Average across data points

  results
end

get '/hotspots' do
  return 400 unless params[:host] # Bad request if no host is given

  host = params[:host]
  session = Moped::Session.new([ "127.0.0.1:27017" ])
  session.use "hotspots"
  toppages = session[host].find({}).to_a.map{|t| t["toppages"]}

  return 404 if toppages.empty? # No records found for this host

  earlier_results = extract_results(toppages[0..(toppages.length / 2 - 1)])
  later_results = extract_results(toppages[(toppages.length / 2)..-1])

  later_results.values.each do |page|
    page.visitors -= earlier_results[page.path].visitors if earlier_results[page.path]
  end.reject{|p| p.visitors <= 0}.sort{|a,b| b.visitors <=> a.visitors}.to_json
end