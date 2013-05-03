require 'bundler/setup'
require 'httparty'
require 'json'
require 'moped'
require 'rufus/scheduler'

API_URL = "http://api.chartbeat.com/live/toppages/"
API_KEY = "317a25eccba186e0f6b558f45214c0e7"

def toppages_url_for(host, limit = 100)
  "#{API_URL}?apikey=#{API_KEY}&host=#{host}&limit=#{limit}"
end

def get_toppages(host)
  toppages = JSON.parse(HTTParty.get(toppages_url_for(host)).body)
  session = Moped::Session.new([ "127.0.0.1:27017" ])
  session.use "hotspots"
  session[host].insert({ :toppages => toppages })
end

hosts = JSON.parse(HTTParty.get("https://s3.amazonaws.com/interview-files/hosts.json").body)

scheduler = Rufus::Scheduler.start_new

puts "Polling toppages every 5 seconds for: #{hosts.to_s}"

hosts.each do |host|
  session = Moped::Session.new([ "127.0.0.1:27017" ])
  session.use "hotspots"
  unless session.collections.map(&:name).include?(host)
    session.command(
        create: host, # Collection has name of host
        capped: true, # Create a capped collection
        max: 24, # maximum 24 records is 2 minutes of toppages (requesting every 5 seconds)
        size: 10485760) # 10 MB should be plenty when there's a limit of 24 documents per collection
  end
  scheduler.every '5s' do
    get_toppages(host)
  end
end

scheduler.join