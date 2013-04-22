Gem::Specification.new do |s|
  s.name        = 'precog'
  s.version     = '1.0.0.pre'
  s.date        = '2013-04-22'
  s.summary     = "Precog Client"
  s.description = "Client library for the Precog platform"
  s.authors     = ["Daniel Spiewak"]
  s.email       = 'daniel@precog.com'
  s.files       = ["lib/precog.rb"]
  s.homepage    = 'https://www.precog.com'
  
  s.add_runtime_dependency 'addressable', ['~> 2.3']
  s.add_development_dependency 'rspec', ['~> 2.13']
end
