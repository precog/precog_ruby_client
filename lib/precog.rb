require 'addressable/uri'

require 'net/http'
require 'net/https'

require 'json'

module Precog

  # The Precog Beta HTTPS service. This is also the default Precog service
  # used when one isn't specified. If you signed up for a beta account, this
  # is the services you'll want to use.
  DEFAULT_HOST = 'beta.precog.com'

  # This is the path used by default to validate the certificate returned
  # on https connections.
  ROOT_CA = '/etc/ssl/certs'

  # Precog API version being used.
  VERSION = 1

  # Struct used to flag use of the CSV format with the specified line separator,
  # record delimiter and literal quote.  The default separator is <tt>\n</tt>, the
  # default delimiter is <tt>,</tt>, while the default quote is <tt>"</tt>.
  # These defaults are precisely what are specified in the DEFAULT_CSV value.
  CSV = Struct.new :separator, :delimiter, :quote
  DEFAULT_CSV = CSV.new("\n", ',', '"')

  # Struct used to return account information from the Client class.
  AccountInfo = Struct.new :api_key, :account_id, :email

  # Represents an error or warning returned from the Precog API.  The position
  # information is optional, and is reprensented by an instance of +Position+
  # when present.
  Info = Struct.new :message, :position

  # Represents a location within a Quirrel source fragment.
  Position = Struct.new :line_num, :column_num, :line_text

  # A simple REST client for storing data in Precog and querying it with Quirrel.
  #
  # This provides methods to upload files to
  # your virtual file system (in Precog), append records/events to the VFS,
  # delete data, and run Quirrel queries on your data. Additionally, you can
  # also create new accounts and get the account details for existing accounts.
  #
  # All methods are blocking, which means that the method returns when the
  # server has replied with the answer.
  class Client
    attr_reader :api_key, :account_id, :host, :base_path, :port

    # Builds a new client to connect to precog services.  Accepts an optional
    # +Hash+ of options of the following type:
    #
    # * :host (String)  The Precog service endpoint (default: <tt>'beta.precog.com'</tt>)
    # * :base_path (String) The default root for all actions (default: <tt>"#{account_id}/"</tt>)
    # * :secure (Boolean) Flag indicating whether or not to use HTTPS (default: +true+)
    # * :port (Fixnum) Service endpoint port to use (default: <tt>443</tt>)
    #
    # Arguments:
    #   api_key: (String)
    #   account_id: (String)
    #   options: (Hash)
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

    # Creates a new account ID, accessible by the specified email address and
    # password, or returns the existing account ID. You _must_ provide a
    # service that uses HTTPS to use this service, otherwise an exception will
    # be thrown.  Returns an instance of AccountInfo.
    #
    # Accepts the same options as new.
    #
    # Arguments:
    #   email: (String)
    #   password: (String)
    #   profile: (Object)
    #   options: (Hash)
    def self.create_account(email, password, profile, options = {})
      stub_client = Client.new(nil, nil, options)   # invalid stub client not meant to escape

      raise 'create_account requires https' unless stub_client.secure?

      body = {
        :email => email,
        :password => password,
        :profile => profile
      }

      results = JSON.parse Precog.post(stub_client, "/accounts/v#{VERSION}/accounts/", body.to_json, { 'Content-Type' => 'application/json' }, {}).body
      account_id = results["accountId"]
      account_details(email, password, account_id, options)
    end

    # Retrieves the details about a particular account. This call is the
    # primary mechanism by which you can retrieve your master API key. You
    # *must* provide a service that uses HTTPS to use this service,
    # otherwise an exception will be thrown.  Returns an instance of AccountInfo.
    #
    # Accepts the same options as new.
    #
    # Arguments:
    #   email: (String)
    #   password: (String)
    #   account_id: (String)
    #   options: (Hash)
    def self.account_details(email, password, account_id, options = {})
      stub_client = Client.new(nil, account_id, options)   # invalid stub client not meant to escape

      raise 'account_details requires https' unless stub_client.secure?

      resp = Precog.get_auth(stub_client, "/accounts/v#{VERSION}/accounts/#{account_id}", { 'Content-Type' => 'application/json' }, email, password)
      results = JSON.parse resp.body
      AccountInfo.new(results["apiKey"], account_id, email)
    end

    # Store the object data as a record in Precog. It is serialized by the
    # +to_json+ function.  Returns a pair, <tt>[ingested, errors]</tt>, consisting of
    # a +Fixnum+ and an +Array+ of errors.
    #
    # Note: Calling this method guarantees the object is stored in the Precog
    # transaction log.
    #
    # Arguments:
    #   path: (String)
    #   data: (Object)
    def append(path, data)
      append_all(path, [data])
    end

    # Append a collection of records in Precog.
    #
    # Arguments:
    #   path: (String)
    #   collection: (Array)
    def append_all(path, collection)
      append_raw(path, :json, collection.to_json)
    end

    # Appends all the events in +data+, a string whose format
    # is described by the +format+ argument, to path in the virtual
    # file-system.  The +format+ must be in the following set:
    #
    # * :json (raw, well-formed JSON)
    # * :json_stream (well-formed JSON values separated by newlines)
    # * :csv (comma-separated values, delimited by newlines using the " character for quoting)
    # * <tt>CSV.new(...)</tt> (an instance of the CSV struct)
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

    # Appends all the events in +file+, a file whose format is described by
    # +format+ (see append_raw), to +path+ in the virtual file-system.
    #
    # For instance, to ingest a CSV file, you could do something like:
    #
    #   client = Precog.new(api_key, account_id)
    #   csv_file = '/path/to/my.csv'
    #   client.append_from_file('my.csv', :csv, csv_file)
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

    # Uploads the records in +file+ to +path+. This is equivalent
    # to first _deleting the data_ at the VFS path (using delete), then
    # calling append_from_file.
    def upload_file(path, format, file)
      delete path
      append_from_file(path, format, file)
    end

    # Deletes the data stored at the specified path. This does NOT do a
    # recursive delete. It'll only delete the data the path specified, all
    # other data in sub-paths of +path+ will remain intact.
    def delete(path)
      path = relativize_path path

      Precog.connect self do |http|
        uri = Addressable::URI.new
        uri.query_values = { :apiKey => api_key }

        http.delete "/ingest/v#{VERSION}/fs/#{path}?#{uri.query}"
      end
    end

    # Executes a synchronous query relative to the specified base path. The
    # HTTP connection will remain open for as long as the query is evaluating
    # (potentially minutes).
    #
    # Not recommended for long-running queries, because if the connection is
    # interrupted, there will be no way to retrieve the results of the query.
    #
    # Returns a triple of errors, warnings and an +Array+ of data representing
    # the query results.  The errors and warnings are represented as +Array+(s)
    # of +Info+ objects.
    def query(path, query)
      path = relativize_path path
      params = { :apiKey => api_key, :q => query, :format => 'detailed' }
      resp = Precog.get(self, "/analytics/v#{VERSION}/fs/#{path}", { 'Content-Type' => 'application/json' }, params)
      output = JSON.parse resp.body

      [
        output["errors"].select { |i| !i.nil? }.map { |i| Precog.parse_info i },
        output["warnings"].select { |i| !i.nil? }.map { |i| Precog.parse_info i },
        output["data"]
      ]
    end

    # Runs an asynchronous query against Precog. An async query is a query
    # that simply returns a Job ID, rather than the query results. You can
    # then periodically poll for the results of the job/query.
    #
    # This does _NOT_ run the query in a new thread. It will still block
    # the current thread until the server responds.
    #
    # An example of using query_async to poll for results
    # could look like:
    #
    #   client = ...
    #   query = client.query_async("foo/", "min(//bar)")
    #
    #   result = nil
    #   result = query.parsed until result
    #   errors, warnings, data = result
    #   min = data.first
    #   puts "Minimum is: #{min}"
    #
    # This is ideal for long running queries.
    #
    # Returns a Query object.
    def query_async(path, query)
      path = relativize_path path
      params = { :apiKey => api_key, :q => query, :prefixPath => path }
      resp = Precog.post(self, "/analytics/v#{VERSION}/queries", '', { 'Content-Type' => 'application/json' }, params)
      output = JSON.parse resp.body
      Query.new(self, output['jobId'])
    end

    private

    def relativize_path(path)
      (base_path + path).gsub(/\/+/, '/')
    end
  end

  # Accessor object for a currently-running asynchronous query.  This class
  # provides methods to access the status of the job, as well as retrieve the
  # results once the job has completed.
  class Query
    attr_reader :client, :qid

    # Creates a new Query with the given Client and batch query ID.  This is
    # invoked by the query_async method in Client.
    #
    # Arguments:
    #   client: (Client)
    #   qid: (String)
    def initialize(client, qid)
      @client = client
      @qid = qid
    end

    # NOT IMPLEMENTED
    def status
      raise 'not implemented'
    end

    # This polls Precog for the completion of an async query. If the query
    # has completed, then a triple of errors, warnings and data is returned.
    # Otherwise, +nil+ is returned.
    def parsed
      params = { :apiKey => client.api_key }
      resp = Precog.get(client, "/analytics/v#{VERSION}/queries/#{qid}", { 'Content-Type' => 'application/json' }, params)
      data = resp.body

      if data && data.length >= 2
        output = JSON.parse data

        [
          output["errors"].select { |i| !i.nil? }.map { |i| Precog.parse_info i },
          output["warnings"].select { |i| !i.nil? }.map { |i| Precog.parse_info i },
          output["data"]
        ]
      end
    end
  end

  class << self
    def parse_info(info)   # :nodoc:
      pos_hash = info['position']
      pos = Position.new pos_hash['line'].to_i, pos_hash['column'].to_i, pos_hash['text'] unless pos_hash.nil?
      Info.new info['message'], pos
    end

    def post(client, path, body, header, params = {})  # :nodoc:
      connect client do |http|
        uri = Addressable::URI.new
        uri.query_values = params

        http.post("#{path}?#{uri.query}", body, header)
      end
    end

    def get(client, path, header, params = {})  # :nodoc:
      get_auth(client, path, header, nil, nil, params)
    end

    def get_auth(client, path, header, user, pass, params = {})  # :nodoc:
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

    def connect(client)  # :nodoc:
      http = Net::HTTP.new(client.host, client.port)
      http.use_ssl = client.secure?

      if (File.directory?(ROOT_CA) && http.use_ssl?)
        http.ca_path = ROOT_CA
        http.verify_mode = OpenSSL::SSL::VERIFY_PEER
        http.verify_depth = 5
      end

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
