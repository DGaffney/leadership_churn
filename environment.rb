require 'mongo_mapper'
require 'json'
require 'pry'
require 'sinatra'
require 'bcrypt'
require 'sinatra/flash'
require 'sidekiq'
require 'sidekiq/web'
require 'sidekiq/api'

MongoMapper.connection = Mongo::MongoClient.new(SETTINGS["mongo_host"], SETTINGS["mongo_port"], :pool_size => 25, :pool_timeout => 60)
MongoMapper.database = SETTINGS["mongo_db"]

Dir[File.dirname(__FILE__) + '/extensions/*.rb'].each {|file| require file }
Dir[File.dirname(__FILE__) + '/helpers/*.rb'].each {|file| require file }
Dir[File.dirname(__FILE__) + '/handlers/*.rb'].each {|file| require file }
Dir[File.dirname(__FILE__) + '/before_hooks/*.rb'].each {|file| require file }
Dir[File.dirname(__FILE__) + '/lib/*.rb'].each {|file| require file }
Dir[File.dirname(__FILE__) + '/models/*.rb'].each {|file| require file }
Dir[File.dirname(__FILE__) + '/tasks/*.rb'].each {|file| require file }


set :erb, :layout => :'layouts/main'
enable :sessions
helpers Sinatra::Partials