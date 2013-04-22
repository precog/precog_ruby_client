require 'addressable/uri'

require 'net/http'
require 'net/https'

require 'json'

module Precog
  DEFAULT_HOST = 'beta.precog.com'
  VERSION = 1
  
  CSV = Struct.new :separator, :delimiter, :quote
  DEFAULT_CSV = CSV.new("\n", ',', '"')
  
  AccountInfo = Struct.new :api_key, :account_id, :email
  
  class Client
    attr_reader :api_key, :account_id, :host, :base_path, :port
    
    def initialize(api_key, account_id, options = {})
      options[:host] ||= DEFAULT_HOST
      options[:base_path] ||= "#{account_id}/"
      options[:secure] = true if options[:secure].nil?
      options[:port] ||= options[:secure] ? 443 : 80
      
      @api_key = api_key
      @account_id = account_id
      @host = options[:host]
      @base_path = options[:base_path]
      @secure = options[:secure]
      @port = options[:port]
    end
    
    def secure?
      @secure
    end
    
    def self.create_account(email, password, profile, options = {})
      stub_client = Client.new(nil, nil, options)   # invalid stub client not meant to escape
      
      raise 'create_account requires https' unless stub_client.secure?
      
      body = {
        :email => email,
        :password => password,
        :profile => profile
      }
      
      results = JSON.parse Precog.post(stub_client, "/accounts/v#{VERSION}/accounts/", body.to_json, {}).body
      account_id = results["accountId"]
      account_details(email, password, account_id, options)
    end
    
    def self.account_details(email, password, account_id, options = {})
      stub_client = Client.new(nil, account_id, options)   # invalid stub client not meant to escape
      
      raise 'account_details requires https' unless stub_client.secure?
      
      resp = Precog.get_auth(stub_client, "/accounts/v#{VERSION}/accounts/#{account_id}", { 'Content-Type' => 'application/json' }, email, password)
      results = JSON.parse resp.body
      AccountInfo.new(results["apiKey"], account_id, email)
    end
    
    def append(path, data)
      append_all(path, [data])
    end
    
    def append_all(path, collection)
      append_raw(path, :json, collection.to_json)
    end
    
    # format \in { :json, :json_stream, :csv, CSV.new(...) }
    def append_raw(path, format, data)
      path = relativize_path path
      
      content_type = if format == :json then
        'application/json'
      elsif format == :json_stream
        'application/x-json-stream'
      elsif CSV === format
        'text/csv'
      elsif format == :csv
        format = DEFAULT_CSV
        'text/csv'
      else
        raise "invalid format: #{format}"   # todo
      end
      
      csv_params = if CSV == format then
        { :escape => CSV.escape, :delimiter => CSV.delimiter, :quote => CSV.quote }
      else
        {}
      end
      
      header = { 'Content-Type' => content_type }
      params = { :apiKey => api_key, :receipt => true, :mode => 'batch' }.merge csv_params
      resp = Precog.post(self, "/ingest/v#{VERSION}/fs/#{path}", data, header, params)
      
      results = JSON.parse resp.body
      [results["ingested"], results["errors"]]
    end
    
    # takes a thingy implementing #each
    def append_stream(path, format, stream)
      path = relativize_path path
      
      content_type = if format == :json then
        'application/json'
      elsif format == :json_stream
        'application/x-json-stream'
      elsif CSV === format
        raise 'cannot perform streaming ingest in CSV format'
        'text/csv'
      elsif format == :csv
        raise 'cannot perform streaming ingest in CSV format'
        format = DEFAULT_CSV
        'text/csv'
      else
        raise "invalid format: #{format}"   # todo
      end
      
      csv_params = if CSV == format then
        { :escape => CSV.escape, :delimiter => CSV.delimiter, :quote => CSV.quote }
      else
        {}
      end
      
      header = { 'Content-Type' => content_type }
      params = { :apiKey => api_key, :receipt => true }.merge csv_params
      
      stream.map do |chunk|
        resp = Precog.post(self, "/ingest/v#{VERSION}/fs/#{path}", chunk, header, params)
      
        results = JSON.parse resp.body
        [results["ingested"], results["errors"]]
      end
    end
    
    def append_from_file(path, format, file)
      path = relativize_path path
      
      content_type = if format == :json then
        'application/json'
      elsif format == :json_stream
        'application/x-json-stream'
      elsif CSV === format
        'text/csv'
      elsif format == :csv
        format = DEFAULT_CSV
        'text/csv'
      else
        raise "invalid format: #{format}"   # todo
      end
      
      csv_params = if CSV == format then
        { :escape => CSV.escape, :delimiter => CSV.delimiter, :quote => CSV.quote }
      else
        {}
      end
      
      header = { 'Content-Type' => content_type }
      params = { :apiKey => api_key, :receipt => true, :mode => 'batch' }.merge csv_params
      
      File.open(file, 'r') do |stream|
        back = []
        stream.each_line do |chunk|
          resp = Precog.post(self, "/ingest/v#{VERSION}/fs/#{path}", chunk, header, params)
      
          results = JSON.parse resp.body
          back << [results["ingested"], results["errors"]]
        end
        back
      end
    end
    
    def upload_file(path, format, file)
      delete path
      append_from_file(path, format, file)
    end
    
    # does not provide recursive delete
    def delete(path)
      path = relativize_path path
      
      Precog.connect self do |http|
        uri = Addressable::URI.new
        uri.query_values = { :apiKey => api_key }
        
        http.delete "/ingest/v#{VERSION}/fs/#{path}?#{uri.query}"
      end
    end
    
    # returns [errors, warnings, results]
    def query(path, query)
      path = relativize_path path
      params = { :apiKey => api_key, :q => query, :format => 'detailed' }
      resp = Precog.get(self, "/analytics/v#{VERSION}/fs/#{path}", { 'Content-Type' => 'application/json' }, params)
      output = JSON.parse resp.body
      [output["errors"], output["warnings"], output["data"]]
    end
    
    # returns a Query object
    def query_async(path, query)
      path = relativize_path path
      params = { :apiKey => api_key, :q => query, :prefixPath => path }
      resp = Precog.post(self, "/analytics/v#{VERSION}/queries", '', { 'Content-Type' => 'application/json' }, params)
      output = JSON.parse resp.body
      Query.new(self, output['jobId'])
    end
    
    def relativize_path(path)
      (base_path + path).gsub(/\/+/, '/')
    end
  end
  
  class Query
    attr_reader :client, :qid
    
    def initialize(client, qid)
      @client = client
      @qid = qid
    end
    
    def status
      raise 'not implemented'
    end
    
    def parsed
      output = JSON.parse raw
      [output["errors"], output["warnings"], output["data"]]
    end
    
    def raw
      params = { :apiKey => client.api_key }
      resp = Precog.get(client, "/analytics/v#{VERSION}/queries/#{qid}", { 'Content-Type' => 'application/json' }, params)
      resp.body
    end
  end
  
  class << self
    def post(client, path, body, header, params = {})
      connect client do |http|
        uri = Addressable::URI.new
        uri.query_values = params
        
        http.post("#{path}?#{uri.query}", body, header)
      end
    end
    
    def get(client, path, header, params = {})
      get_auth(client, path, header, nil, nil, params)
    end
    
    def get_auth(client, path, header, user, pass, params = {})
      connect client do |http|
        uri = Addressable::URI.new
        uri.query_values = params
        
        req = Net::HTTP::Get.new("#{path}?#{uri.query}")
        
        header.each do |key, value|
          req[key] = value
        end
        
        req.basic_auth(user, pass) if user && pass
        
        http.request req
      end
    end
    
    def connect(client)
      http = Net::HTTP.new(client.host, client.port)
      http.use_ssl = client.secure?
      http.start
      
      result = nil
      begin
        result = yield http
      ensure
        http.finish
      end
      
      result
    end
  end
end
