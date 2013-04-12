require 'rubygems'

require 'net/http'
require 'net/https'

require 'json'

module Precog
  DEFAULT_HOST = 'beta.precog.com'
  
  class Client
    attr_reader :api_key, :account_id, :host, :base_path, :secure
    
    def initialize(api_key, account_id, options = {})
      options[:host] ||= DEFAULT_HOST
      options[:base_path] ||= "#{account_id}/"
      options[:secure] ||= true
      
      @api_key = api_key
      @account_id = account_id
      @host = options[:host]
      @base_path = options[:base_path]
      @secure = options[:secure]
    end
    
    def create_account(email, password, profile)
      raise 'not implemented'
    end
    
    def account_details
      raise 'not implemented'
    end
    
    def append(path, data)
      raise 'not implemented'
    end
    
    def append_all(path, collection)
      raise 'not implemented'
    end
    
    # format \in { :json, :json_stream, CSV.new(...) }
    def append_raw(path, format, data)
      raise 'not implemented'
    end
    
    # takes a block, passing a sink
    def append_stream(path, format)
      raise 'not implemented'
    end
    
    def append_from_file(path, format, file)
      raise 'not implemented'
    end
    
    def upload_file(path, format, file)
      raise 'not implemented'
    end
    
    def delete(path)
      raise 'not implemented'
    end
    
    # returns query status; takes a block which is fed data
    def query(path, query)
      raise 'not implemented'
    end
    
    def query_raw(path, query)
      raise 'not implemented'
    end
    
    # returns a Query object
    def query_async(path, query)
      raise 'not implemented'
    end
  end
  
  class Query
    attr_reader :qid
    
    def initialize(qid)
      @qid = qid
    end
    
    def status
      raise 'not implemented'
    end
    
    def results
      raise 'not implemented'
    end
    
    def raw
      raise 'not implemented'
    end
  end
  
  private
  
  class Sink
    def puts(str)
      raise 'not implemented'
    end
    
    def <<(str)
      puts str
    end
  end
  
  CSV = Struct.new :separator, :delimiter, :quote
end
