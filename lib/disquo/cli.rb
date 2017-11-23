require 'singleton'
require 'optparse'
require 'yaml'
require 'erb'

module Disquo
  class CLI
    include Singleton

    DEFAULT_OPTIONS = {
      config:       nil,
      queues:       ["default"],
      concurrency:  10,
      disque_nodes: ["127.0.0.1:7711"],
      disque_opts:  {},
      pool_size:    nil, # auto
      pool_timeout: 1,
      logfile:      nil, # STDOUT
      wait_time:    1,
      wait_count:   100,
    }

    attr_reader :opts

    def initialize
      @opts = DEFAULT_OPTIONS.dup
    end

    def parse!(argv = ARGV)
      parser.parse!(argv)

      # Check config file
      if opts[:config] && !File.exist?(opts[:config])
        raise ArgumentError, "Unable to find config file in #{opts[:config]}"
      end

      # Load config file
      if opts[:config]
        conf = YAML.load(ERB.new(IO.read(opts[:config])).result)
        unless conf.is_a?(Hash)
          raise ArgumentError, "File in #{opts[:config]} does not contain a valid configuration"
        end
        conf.each do |key, value|
          opts[key.to_sym] = value
        end
      end

      # Set pool size using concurrency
      opts[:pool_size] ||= opts[:concurrency] + 5
    end

    def run!
      return if @worker

      require opts[:require] if opts[:require]
      require 'disquo/worker'

      Disquo.logger = ::Logger.new(opts[:logfile]) if opts[:logfile]
      disque = Disquo.connect \
        nodes: opts[:disque_nodes],
        opts:  opts[:disque_opts],
        pool_size:    opts[:pool_size],
        pool_timeout: opts[:pool_timeout]

      Signal.trap("TERM") { shutdown }
      Signal.trap("INT")  { shutdown }

      @worker = Disquo::Worker.new disque,
        queues: opts[:queues],
        concurrency: opts[:concurrency],
        wait_time:   opts[:wait_time],
        wait_count:  opts[:wait_count]
      @worker.run
      @worker.wait
    end

    def shutdown
      return unless @worker

      @worker.shutdown
    end

    def parser
      @parser ||= begin
        op = OptionParser.new do |o|
          o.on '-C', '--config FILE', "YAML config file to load" do |v|
            @opts[:config] = v
          end

          o.on '-r', '--require [PATH|DIR]', "File to require" do |v|
            @opts[:require] = v
          end

          o.on '-L', '--logfile PATH', "path to writable logfile" do |v|
            @opts[:logfile] = v
          end
        end

        op.banner = "disquo [options]"
        op.on_tail "-h", "--help", "Show help" do
          $stdout.puts parser
          exit 1
        end
      end
    end

  end
end

