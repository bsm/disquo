require 'spec_helper'

RSpec.describe Disquo::Worker do

  subject do
    described_class.new Disquo.connect, wait_time: 0.1, queues: Disquo::TEST::QUEUE
  end

  it "should run/process/shutdown" do
    runner = Thread.new { subject.run }
    runner.abort_on_exception = true

    # seed 200 jobs
    200.times {|n| TestJob.enqueue(n) }
    wait_for { !qlen.zero? }

    # ensure runner processes them all
    wait_for { qlen.zero? }
    expect(runner).to be_alive
    expect(qlen).to eq(0)

    # ask runner to quit
    expect(subject.shutdown).to be_truthy
    expect(subject.shutdown).to be_falsey

    # wait for runner to exit
    subject.wait
    expect(runner).not_to be_alive

    # check what's been performed
    expect(Disquo::TEST::PERFORMED.size).to eq(200)
    expect(Disquo::TEST::PERFORMED.last).to include(
      klass: "TestJob",
      queue: "__disquo_test__",
    )
    expect(Disquo::TEST::PERFORMED.last[:job_id]).to match(/^D[\w\-]+/)
    expect(Disquo::TEST::PERFORMED.map {|e| e[:args].first }).to match_array(0..199)
  end

  def wait_for
    20.times do
      break if yield
      sleep(0.1)
    end
  end

end
