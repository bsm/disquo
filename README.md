# Disquo

[![Build Status](https://travis-ci.org/bsm/disquo.png?branch=master)](https://travis-ci.org/bsm/disquo)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Minimalist, threaded high-performance Ruby workers on top of [Disque](https://github.com/antirez/disque).

## Installation

Add this to your Gemfile:

```ruby
gem 'disquo'
```

Then execute:

```shell
$ bundle
```

## Usage

Define a job:

```ruby
require 'disquo'

class MyJob
  include Disquo::Job

  job_options queue: "notdefault", ttl: 3600, async: true

  def perform(msg)
    $stdout.puts "Hello #{msg}!"
  end
end

# Enqueue with override
MyJob.enqueue ["World"], ttl: 7200
```

Create a worker config file:

```yaml
queues: ["default", "notdefault"]
concurrency: <%= ENV['NUM_THREADS'] || 20 %>
```

Start a worker:

```shell
$ RACK_ENV=production disquo -C config/disquo.yaml -r config/environment.rb
I, [#12581]  INFO -- : Starting worker - queues: ["default", "notdefault"], concurrency: 20
I, [#12581]  INFO -- : Process {"klass":"MyJob","args":["World"]} - thread: 7807s, job: DI8613f71b34be272dff91e63fa576340076f169bf05a0SQ
...
```
