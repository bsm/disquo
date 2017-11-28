require 'rspec'
require 'disquo'
require 'disquo/worker'

require 'active_job'
require 'active_job/queue_adapters/disquo_adapter'
ActiveJob::Base.queue_adapter = :disquo
ActiveJob::Base.logger = Logger.new(nil)

module Disquo::TEST
  QUEUE = "__disquo_test__"
  PERFORMED = []
end

class TestJob
  include Disquo::Job

  job_options queue: Disquo::TEST::QUEUE, async: true

  def perform(*args)
    Disquo::TEST::PERFORMED.push(klass: self.class.name, args: args, queue: queue, job_id: job_id)
  end
end

class TestActiveJob < ActiveJob::Base
  queue_as Disquo::TEST::QUEUE

  def perform(*args)
    Disquo::TEST::PERFORMED.push(klass: self.class.name, args: args)
  end

  def serialize
    super.merge('disquo' => {'ttl' => 3600})
  end
end

helpers = Module.new do

  def qlen
    Disquo.disque.with do |conn|
      conn.call :qlen, Disquo::TEST::QUEUE
    end
  end

  def show(job_id)
    Disquo.disque.with do |conn|
      pairs = conn.call :show, job_id
      Hash[*pairs]
    end
  end

end

RSpec.configure do |c|
  c.include helpers

  c.before :each do
    Disquo.logger = Logger.new(nil)
  end

  c.after :each do
    Disquo::TEST::PERFORMED.clear
    Disquo.disque.with do |conn|
      _, ids = conn.call :jscan, 0, :count, 10000, :queue, Disquo::TEST::QUEUE
      conn.call :deljob, *ids unless ids.empty?
    end
  end

end
