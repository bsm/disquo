# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name        = "disquo"
  s.version     = "0.3.1"
  s.platform    = Gem::Platform::RUBY

  s.licenses    = ["Apache-2.0"]
  s.summary       = "Concurrent background workers on top of Disque"
  s.description   = "Concurrent background workers on top of Disque"

  s.authors     = ["Dimitrij Denissenko"]
  s.email       = "dimitrij@blacksquaremedia.com"
  s.homepage    = "https://github.com/bsm/disquo"

  s.executables   = ['disquo']
  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- spec/*`.split("\n")
  s.require_paths = ["lib"]
  s.required_ruby_version = ">= 2.3.0"

  s.add_dependency 'disque'
  s.add_dependency 'connection_pool'
  s.add_dependency 'concurrent-ruby'

  s.add_development_dependency 'rake'
  s.add_development_dependency 'rspec'
end
