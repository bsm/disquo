require 'disquo'
require 'concurrent/executor/fixed_thread_pool'
require 'concurrent/atomic/atomic_fixnum'

class Disquo::Worker
  attr_reader :disque, :queues, :wait_time

  # Init a new worker instance
  # @param [ConnectionPool] disque client connection pool
  # @param [Hash] options
  # @option [Array<String>] :queues queues to watch. Default: ["default"]
  # @option [Integer] :concurrency the number of concurrent threads. Default: 10
  # @option [Numeric] :wait_time maximum time (in seconds) to wait for jobs when retrieving next batch. Default: 1s
  def initialize(disque, queues: [Disquo::DEFAULT_QUEUE], concurrency: 10, wait_time: 1, wait_count: 100)
    @disque     = disque
    @queues     = Array(queues)
    @threads    = Concurrent::FixedThreadPool.new(concurrency)
    @busy       = Concurrent::AtomicFixnum.new
    @wait_time  = wait_time
    @wait_count = wait_count
  end

  # Run starts the worker
  def run
    Disquo.logger.info "Worker starting - queues: #{queues.inspect}, concurrency: #{@threads.max_length}"

    begin
      run_cycle
    rescue => e
      handle_exception(e)
    end until @stopped

    Disquo.logger.info "Worker shutting down"
    @threads.shutdown
  end

  # Blocks until worker is stopped
  def wait(timeout = nil)
    @threads.wait_for_termination(timeout)
    Disquo.logger.info "Worker shutdown complete"
  end

  # Stops the worker
  def shutdown
    return false if @stopped

    @stopped = true
  end

  private

    def run_cycle
      jobs = Array(next_batch)
      until @stopped || jobs.empty?
        job = jobs.shift
        schedule(*job)
      end
    ensure
      requeue(jobs) unless jobs.nil? || jobs.empty?
    end

    def next_batch
      count = @threads.max_length - @busy.value
      if count < 1
        sleep(wait_time.fdiv(2))
        return
      end

      jobs = disque.with do |cn|
        cn.fetch from: queues, timeout: (wait_time*1000).to_i, count: count
      end

      @is_down = nil
      jobs
    rescue Errno::ECONNREFUSED => e
      handle_disque_exception e, message: "Failed to retrieve jobs:", notrace: true
      nil
    end

    def schedule(queue, job_id, payload)
      @busy.increment
      @threads.post do
        begin
          perform(queue, job_id, payload)
        ensure
          @busy.decrement
        end
      end
    end

    def perform(queue, job_id, payload)
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
        @is_down = nil
      rescue Errno::ECONNREFUSED => e
        handle_disque_exception e, message: "Failed to ACK job #{job_id}:", notrace: true
        retry unless @stopped
      end
    end

    def requeue(jobs)
      ids = jobs.map {|_, job_id, _| job_id }
      disque.with {|cn| cn.call :enqueue, *ids }
    end

    def handle_disque_exception(e, opts = {})
      if !@is_down
        @is_down = true
        handle_exception(e, opts)
      end
      sleep(1)
    end

    def handle_exception(e, opts = {})
      lines = [
        opts[:message],
        "#{e.class.name}: #{e.message}",
        (opts[:notrace] ? nil : e.backtrace),
      ].compact.flatten

      Disquo.logger.error lines.join("\n")
    end

end
