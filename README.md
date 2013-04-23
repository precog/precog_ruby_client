Precog Client
=============

A Ruby client for working with Precog's REST API.

Getting Started
---------------

The easiest way to install the Ruby client library is via RubyGems:

    $ gem install precog
    
To install pre-releases, append the `--pre` option:

    $ gem install precog --pre

### Quick Start

A good first step is simply to create a `Precog::Client` instance. This is the main
class through which we can access Precog.

    precog = Precog::Client.new(my_api_key, my_account_id)

We don't specify an *end-point* to use here. By default, the Precog client
will use the Precog Beta service as its end-point. If you are using one of our
production offerings, then you'll need to provide the appropriate end-point.
If you signed up for a Beta account, then just replace `my_api_key` and
`my_account_id` with your Precog API key and account ID respectively.

Now we'll want to store some data in Precog. Let's say we have a CSV file,
`my-data.csv`, that looks like this:

    Order ID,Customer ID,Product ID,Quantity,Price,TaxRate
    1234,5678,9101112,2,3.99,0.13
    ...

We'll upload the file to Precog to `my-data`.

    file = 'my-data.csv'
    precog.upload_file('my-data', :csv, file);

Data uploaded/stored in Precog isn't immediately available. Instead, we
guarantee that your data will eventually be made available. Usually this is
nearly instantaneous, however it is something to keep in mind.

So, now we want to query the data. Let's look at some example queries.

Say we want to calculate the total sales of all our data, including tax.
We formulate this as a Quirrel query, then execute it with our Precog client.

    totals = <<query
      data := //my-data
      data with {
        total: data.Quantity * data.Price * (1 + data.TaxRate)
      }
    query
    
    errors, warnings, data = precog.query('', totals)

Query results are returned as a triple of errors, warnings and data. The `errors`
and `warnings` are arrays (ideally empty!), and the `data` is an array of objects
decoded using `JSON.parse`.

    data.each { |d| puts d['total'] }

Although we print the results out, we don't actually know for sure that the
query succeeded. So, we probably want to check. Quirrel errors (eg. syntax
errors) are reported as `Hash`s. We can extract from these hashes information
about the line origin of the error/warning (in the original query) and the exact
error message:

    unless errors.empty?
	  puts 'Query failed!'
	  errors.each do |error|
	    puts "Error: #{error['message']}"
	  end
    }

    warnings.each do |warning|
      puts "Warning: #{warning['message']}"
    end

The `Client` also let's submit queries for execution, without actually
requiring the results right away. This is used for long-running queries, so
we don't have to wait around for the results. Precog calls these *async*
queries. When an async query is run, a `Query` object is returned. This is a
handle that let's us periodically poll Precog to see if the query has finished.

Let's use async queries to find our best customer.

    best_customer = <<query
      salesByCustomer := solve 'customer
        {
          customer: 'customer,
          sales: sum(order.Quantity * order.Price)
        }
        
      bestCustomer := salesByCustomer where
        salesByCustomer.sales = max(salesByCustomer.sales)
        
      bestCustomer
    query

    query = precog.query_async('', bestCustomer)

Now that we have a handle on the results, we can do 1 of 2 things. We can get
the results directly and store them in memory, using `parsed`. In this
case, if the results aren't ready yet, then `parsed` returns `null`. So,
that indicates we need to wait a bit and try again.

    results = nil
    results = query.parsed while results.nil?
    
    errors, warnings, data = results

License
-------

Copyright 2013 Reportgrid, Inc.

Licensed under the MIT License: [http://opensource.org/licenses/MIT]

