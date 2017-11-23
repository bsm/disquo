module Disquo::Job
  attr_accessor :job_id, :queue, :disque

  def self.included(base)
    base.extend(ClassMethods)
  end

  # Indicate to disque that this job is still in progress
  def working!
    disque.with {|cn| cn.call :working, job_id } if disque && job_id
  end

  module ClassMethods

    # Configures the Job
    #
    # @param [Hash] opts the options to enqueue the message with.
    # @option opts [String] :queue a specific queue name. Default: "default"
    # @option opts [Numeric] :timeout number of seconds to wait for server to `:replicate` level (if no `:async` is specified). Default: 10s
    # @option opts [Integer] :replicate number of nodes the job should be replicated to
    # @option opts [Integer] :retry seconds after, if no ACK is received, the job is put into the queue again for delivery. Default: 5m
    # @option opts [Integer] :ttl max job life-time in seconds. Default: 24h
    # @option opts [Integer] :maxlen specifies that if there are already `maxlen` messages queued for the specified queue name, the message is refused.
    # @option opts [Boolean] :async asks the server to let the command return ASAP and replicate the job to other nodes in the background.
    def job_options(opts = {})
      @job_options ||= {}
      @job_options.update(opts) unless opts.nil? || opts.empty?
      @job_options
    end

    # Enqueues the job
    # @param [Array] args arguments to pass to #perform
    # @param [Hash] opts the options to enqueue the message with (see Disquo::Job::ClassMethods.job_options)
    # @option opts [Interger] :delay is the number of seconds that should elapse before the job is queued by any server
    def enqueue(args = [], opts = {})
      opts    = job_options.merge(opts)
      queue   = opts.delete(:queue) || Disquo::DEFAULT_QUEUE
      timeout = ((opts.delete(:timeout) || 10).to_f * 1000).to_i
      payload = Disquo.dump_job(name, args)

      Disquo.disque.with do |conn|
        conn.push(queue, payload, timeout, opts)
      end
    end

  end
end
