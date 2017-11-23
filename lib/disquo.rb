require 'disque'
require 'connection_pool'
require 'json'
require 'logger'

module Disquo
  DEFAULT_QUEUE = "default".freeze

  # Configure disque with a block.
  #
  # @example:
  #   Disquo.configure do |c|
  #     c.disque = c.connect(nodes: ["10.0.0.1:7711", "10.0.0.2:7711", "10.0.0.3:7711"])
  #   end
  def self.configure(&block)
    block.call(self)
  end

  # @option [Array<String>|String] nodes a list of disque nodes. Default: ["127.0.0.1:7711"]
  # @option [Hash] opts additional disque connection options.
  # @option [Numeric] pool_timeout disque connection pool timeout in seconds. Default: 1.0
  # @option [Integer] pool_size the number of slots in the connection pool. Default: 5
  #
  # @return [ConnectionPool] disque client connection pool
  def self.connect(nodes: ["127.0.0.1:7711"], opts: {}, pool_size: 5, pool_timeout: 1)
    ConnectionPool.new timeout: pool_timeout, size: pool_size do
      Disque.new(nodes, opts)
    end
  end

  # @return [ConnectionPool] disque client connection pool
  def self.disque
    @disque ||= connect
  end

  # @param [ConnectionPool] disque client connection pool
  def self.disque=(pool)
    @disque = pool
  end

  # @return [Logger] returns the logger instance
  def self.logger
    @logger ||= Logger.new(STDOUT)
  end

  # @param [Logger] log logger instance to use
  def self.logger=(log)
    @logger = log
  end

  # @param [String] payload
  # @return [Array<Class, Array>] job class and argument
  def self.load_job(payload)
    JSON.load(payload).values_at("klass", "args")
  end

  # @param [String] class_name class name
  # @param [Array] arguments
  # @return [String] serialised job
  def self.dump_job(class_name, args)
    JSON.dump "klass" => class_name, "args" => Array(args)
  end

end

require 'disquo/job'
