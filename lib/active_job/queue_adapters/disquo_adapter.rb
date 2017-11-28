require "disquo"

module ActiveJob
  module QueueAdapters
    # == Disquo adapter for Active Job
    #
    # Read more about Disquo {here}[https://github.com/bsm/disquo].
    #
    # To use queue_adapter config to +:disquo+.
    #
    #   require 'active_job/queue_adapters/disquo_adapter'
    #
    #   Rails.application.config.active_job.queue_adapter = :disquo
    #
    # To pass extra options into the job, you can use custom serialization:
    #
    #    class MyJob < ActiveJob::Base
    #      def serialize
    #        opts = { 'ttl' => 1.hour, 'async' => true }
    #        super.merge('disquo' => opts)
    #      end
    #    end
    class DisquoAdapter

      class JobWrapper #:nodoc:
        include Disquo::Job

        def perform(job_data)
          ActiveJob::Base.execute job_data
        end
      end

      def enqueue(job) #:nodoc:
        enqueue_with_opts job
      end

      def enqueue_at(job, timestamp) #:nodoc:
        delay  = timestamp - Time.current.to_f
        enqueue_with_opts job, delay: delay.to_i
      end

      private

        def enqueue_with_opts(job, opts = {})
          attrs = job.serialize
          opts.update attrs["disquo"].symbolize_keys if attrs["disquo"].is_a?(Hash)
          opts[:queue] = job.queue_name

          job_id = JobWrapper.enqueue [attrs], opts
          job.provider_job_id = job_id
          job_id
        end

    end
  end
end
