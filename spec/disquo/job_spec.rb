require 'spec_helper'

RSpec.describe Disquo::Job do

  it "should enqueue jobs" do
    jid1 = TestJob.enqueue ["foo", 1]
    expect(qlen).to eq(1)
    expect(jid1.size).to eq(48)
    expect(show(jid1)).to include(
      "id"    => jid1,
      "queue" => "__disquo_test__",
      "nacks" => 0,
      "delay" => 0,
      "repl"  => 1,
      "state" => "queued",
      "body"  => %({"klass":"TestJob","args":["foo",1]}),
    )

    jid2 = TestJob.enqueue ["bar"], delay: 600
    expect(qlen).to eq(1)
    expect(jid2.size).to eq(48)
    expect(show(jid2)).to include(
      "id"    => jid2,
      "queue" => "__disquo_test__",
      "nacks" => 0,
      "delay" => 600,
      "repl"  => 1,
      "state" => "active",
      "body"  => %({"klass":"TestJob","args":["bar"]}),
    )
  end

  it "should have instance attributes" do
    job1, job2 = TestJob.new, TestJob.new
    job1.job_id = "JOB1"
    expect(job1.job_id).to eq("JOB1")
    expect(job2.job_id).to be_nil
  end

end
