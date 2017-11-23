require 'disquo'
require 'concurrent'
require 'concurrent/executor/fixed_thread_pool'

class Disquo::Worker
  attr_reader :disque, :queues, :wait_time, :wait_count

  # Init a new worker instance
  # @param [ConnectionPool] disque client connection pool
  # @param [Hash] options
  # @option [Array<String>] :queues queues to watch. Default: ["default"]
  # @option [Integer] :concurrency the number of concurrent threads. Default: 10
  # @option [Numeric] :wait_time maximum time (in seconds) to block for when retrieving next batch. Default: 1s
  # @option [Integer] :wait_count the minimum number of jobs to wait for when retrieving next batch. Default: 100
  def initialize(disque, queues: [Disquo::DEFAULT_QUEUE], concurrency: 10, wait_time: 1, wait_count: 100)
    @disque     = disque
    @queues     = Array(queues)
    @threads    = Concurrent::FixedThreadPool.new(concurrency)
    @wait_time  = wait_time
    @wait_count = wait_count
  end

  # Run starts the worker
  def run
    Disquo.logger.info "Starting worker - queues: #{queues.inspect}, concurrency: #{@threads.max_length}"

    begin
      run_cycle
    rescue => e
      handle_exception(e)
    end until @stopped

    @threads.shutdown
  end

  # Blocks until worker is stopped
  def wait(timeout = nil)
    Disquo.logger.info "Waiting for worker shutdown"
    @threads.wait_for_termination(timeout)
    Disquo.logger.info "Shutdown complete"
  end

  # Stops the worker
  def shutdown
    return false if @stopped

    @stopped = true
  end

  private

    def run_cycle
      jobs = next_batch

      until @stopped || jobs.empty?
        job = jobs.shift
        perform(*job)
      end
    ensure
      requeue(jobs) unless jobs.nil? || jobs.empty?
    end

    def next_batch
      jobs = disque.with do |cn|
        cn.fetch from: queues, timeout: (wait_time*1000).to_i, count: wait_count
      end
      @is_down = nil
      Array(jobs)
    rescue => e
      if !@is_down
        @is_down = true
        handle_exception(e, message: 'Error retrieving jobs:')
      end
      sleep(1)
      []
    end

    def perform(queue, job_id, payload)
      @threads.post do
        thread_id = Thread.current.object_id.to_s(36)
        Disquo.logger.info { "Process #{payload} - thread: #{thread_id}, job: #{job_id}" }

        begin
          class_name, args = Disquo.load_job(payload)

          job = Object.const_get(class_name).new
          job.disque = disque
          job.queue  = queue
          job.job_id = job_id
          job.perform(*args)
        rescue => e
          handle_exception e, message: "Error processing #{payload} - thread: #{thread_id}, job: #{job_id}:"

          disque.with {|cn| cn.call :nack, job_id }
          return
        end

        begin
          disque.with {|cn| cn.call :ackjob, job_id }
        rescue => e
          handle_exception e, message: "Error ACKing #{payload} - thread: #{thread_id}, job: #{job_id}:"
          return
        end
      end
    end

    def requeue(jobs)
      ids = jobs.map {|_, job_id, _| job_id }
      disque.with {|cn| cn.call :enqueue, *ids }
    end

    def handle_exception(e, opts = {})
      lines = [
        opts[:message],
        "#{e.class.name}: #{e.message}",
        e.backtrace
      ].compact.flatten

      Disquo.logger.error lines.join("\n")
    end

end
