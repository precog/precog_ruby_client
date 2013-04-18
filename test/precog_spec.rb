HOST = 'devapi.precog.com'
EMAIL = "ruby-test-#{rand(1000000).to_i}@precog.com"
PASSWORD = 'password'
PATH = "/test/#{rand(1000000).to_i}"

TIMEOUT = 10         # ten second timeout on polling

def generate_path
  "/test/#{rand(1000000).to_i}"
end

def poll(start, &block)
  (Time.now - start).should < TIMEOUT
  poll(start, &block) unless yield
end

describe Precog do
  before :all do
    account = Precog::Client.create_account(EMAIL, PASSWORD, {}, :host => HOST)
    @account_id = account.account_id
    @api_key = account.api_key
    @client = Precog::Client.new(@api_key, @account_id, :host => HOST)
  end
  
  it 'should append to a random dataset' do
    data = { :data => [{ :v => 1 }, { :v => 2 }] }
    path = generate_path
    
    ingested, errors1 = @client.append(path, data)
    errors2, warnings, results = @client.query('', "count(/#{path})")
    
    ingested.should == 1
    errors1.should == []
    
    poll Time.now do
      errors2, warnings, results = @client.query('', "count(/#{path})")
      results == [1]
    end
  end
  
  it 'should append_raw as :json' do
    data = []
    data << { :data => [{ :v => 1 }, { :v => 2 }] }
    data << { :data => [{ :v => 2 }, { :v => 3 }] }
    
    path = generate_path
    
    ingested, errors1 = @client.append_raw(path, :json, data.to_json)
    
    ingested.should == 2
    errors1.should == []
    
    poll Time.now do
      errors2, warnings, results = @client.query('', "count(/#{path})")
      results == [2]
    end
  end
  
  it 'should append_raw as :json_stream' do
    data = "{\"test\":[{\"v\": 1}, {\"v\": 2}]} {\"test\":[{\"v\": 2}, {\"v\": 3}]}"
    
    path = generate_path
    
    ingested, errors1 = @client.append_raw(path, :json_stream, data)
    
    ingested.should == 2
    errors1.should == []
    
    poll Time.now do
      errors2, warnings, results = @client.query('', "count(/#{path})")
      results == [2]
    end
  end
  
  it 'should append_raw as :csv' do
    data = "a,b,c\n1,2,3\n\n,,tom\n\n"
    
    path = generate_path
    
    ingested, errors1 = @client.append_raw(path, :csv, data)
    
    ingested.should == 4
    errors1.should == []
    
    poll Time.now do
      errors2, warnings, results = @client.query('', "count(/#{path})")
      results == [4]
    end
  end
  
  it 'should delete' do
    path = generate_path
    
    ingested, errors1 = @client.append(path, 42)
    
    ingested.should == 1
    errors1.should == []
    
    poll Time.now do
      errors2, warnings, results = @client.query('', "count(/#{path})")
      results == [1]
    end
    
    @client.delete path
    
    poll Time.now do
      errors2, warnings, results = @client.query('', "count(/#{path})")
      results == [0]
    end
  end
  
  it 'should create an account' do
    email = "ruby-test-#{rand(1000000).to_i}@precog.com"
    account = Precog::Client.create_account(email, PASSWORD, {}, :host => HOST)
    account.should_not == nil
    @account_id.should_not == account.account_id
    account.api_key.should_not == nil
    account.email.should == email
  end
  
  it 'should require https for create_account' do
    email = "ruby-test-#{rand(1000000).to_i}@precog.com"
    
    caught = false
    begin
      Precog::Client.create_account(email, PASSWORD, {}, :host => HOST, :secure => false)
    rescue
      caught = true
    end
    
    caught.should == true
  end
  
  it 'should count a non-existent dataset' do
    errors, warnings, results = @client.query('', 'count(//non-existent)')
    
    errors.should == []
    warnings.should == []
    results.should == [0]
  end
  
  it 'should perform an asynchronous query' do
    path = generate_path
    
    data = []
    data << { :testInt => 1, :testString => "" }
    data << { :testInt => 2, :testString => "" }
    data << { :testInt => 3, :testString => "" }
    
    @client.append_all(path, data)
    
    
    poll Time.now do
      errors, warnings, results = @client.query('', "count(/#{path})")
      results == [3]
    end
    
    query = @client.query_async('', "max((/#{path}).testInt)")
    
    result = nil
    result = query.raw until result
    
    errors, warnings, result = query.parsed
    
    errors.should == []
    warnings.should == []
    result.should == [3]
  end
  
  it 'should parse out query warnings' do
    query = "data := \"unused\" 1234"
    errors, warnings, results = @client.query('', query)
    
    errors.should == []
    warnings.size.should > 0
    results.should == [1234]
  end
  
  it 'should parse out query errors' do
    query = '1,2#@'
    errors, warnings, results = @client.query('', query)
    
    errors.size.should > 0
    warnings.should == []
    results.should == []
  end
end
